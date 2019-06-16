/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.KeyValue;
import io.openmessaging.ServiceLifeState;
import io.openmessaging.ServiceLifecycle;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import io.openmessaging.rocketmq.utils.OMSClientUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;

class LocalMessageCache implements ServiceLifecycle {
    private final BlockingQueue<ConsumeRequest> consumeRequestCache;
    private final Map<String, ConsumeRequest> consumedRequest;
    private final ConcurrentHashMap<MessageQueue, Long> pullOffsetTable;
    private final DefaultMQPullConsumer rocketmqPullConsumer;
    private final ClientConfig clientConfig;
    private final ScheduledExecutorService cleanExpireMsgExecutors;
    private ServiceLifeState currentState;

    private final static InternalLogger log = ClientLogger.getLog();

    LocalMessageCache(final DefaultMQPullConsumer rocketmqPullConsumer, final ClientConfig clientConfig) {
        consumeRequestCache = new LinkedBlockingQueue<>(clientConfig.getRmqPullMessageCacheCapacity());
        this.consumedRequest = new ConcurrentHashMap<>();
        this.pullOffsetTable = new ConcurrentHashMap<>();
        this.rocketmqPullConsumer = rocketmqPullConsumer;
        this.clientConfig = clientConfig;
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "OMS_CleanExpireMsgScheduledThread_"));
        this.currentState = ServiceLifeState.INITIALIZED;
    }

    int nextPullBatchNums() {
        return Math.min(clientConfig.getRmqPullMessageBatchNums(), consumeRequestCache.remainingCapacity());
    }

    long nextPullOffset(MessageQueue remoteQueue) {
        if (!pullOffsetTable.containsKey(remoteQueue)) {
            try {
                pullOffsetTable.putIfAbsent(remoteQueue,
                    rocketmqPullConsumer.fetchConsumeOffset(remoteQueue, false));
            } catch (MQClientException e) {
                log.error("A error occurred in fetch consume offset process.", e);
            }
        }
        return pullOffsetTable.get(remoteQueue);
    }

    void updatePullOffset(MessageQueue remoteQueue, long nextPullOffset) {
        pullOffsetTable.put(remoteQueue, nextPullOffset);
    }

    void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException ignore) {
        }
    }

    MessageExt poll() {
        return poll(clientConfig.getOperationTimeout());
    }

    MessageExt poll(final KeyValue properties) {
        int currentPollTimeout = clientConfig.getOperationTimeout();
        if (properties.containsKey(NonStandardKeys.TIMEOUT)) {
            currentPollTimeout = properties.getInt(NonStandardKeys.TIMEOUT);
        }
        return poll(currentPollTimeout);
    }

    private MessageExt poll(long timeout) {
        try {
            ConsumeRequest consumeRequest = consumeRequestCache.poll(timeout, TimeUnit.MILLISECONDS);
            if (consumeRequest != null) {
                MessageExt messageExt = consumeRequest.getMessageExt();
                consumeRequest.setStartConsumeTimeMillis(System.currentTimeMillis());
                MessageAccessor.setConsumeStartTimeStamp(messageExt, String.valueOf(consumeRequest.getStartConsumeTimeMillis()));
                consumedRequest.put(messageExt.getMsgId(), consumeRequest);
                return messageExt;
            }
        } catch (InterruptedException ignore) {
        }
        return null;
    }

    List<MessageExt> batchPoll(KeyValue properties) {
        List<ConsumeRequest> consumeRequests = new ArrayList<>(clientConfig.getRmqPullMessageBatchNums());
        long timeout = properties.getLong(NonStandardKeys.TIMEOUT);
        while (timeout >= 0) {
            int n = consumeRequestCache.drainTo(consumeRequests, clientConfig.getRmqPullMessageBatchNums());
            if (n > 0) {
                List<MessageExt> messageExts = new ArrayList<>(n);
                for (ConsumeRequest consumeRequest : consumeRequests) {
                    MessageExt messageExt = consumeRequest.getMessageExt();
                    consumeRequest.setStartConsumeTimeMillis(System.currentTimeMillis());
                    MessageAccessor.setConsumeStartTimeStamp(messageExt, String.valueOf(consumeRequest.getStartConsumeTimeMillis()));
                    consumedRequest.put(messageExt.getMsgId(), consumeRequest);
                    messageExts.add(messageExt);
                }
                return messageExts;
            }
            if (timeout > 0) {
                LockSupport.parkNanos(timeout * 1000 * 1000);
                timeout = 0;
            } else {
                timeout = -1;
            }
        }
        return null;
    }

    void ack(final String messageId) {
        ConsumeRequest consumeRequest = consumedRequest.remove(messageId);
        if (consumeRequest != null) {
            long offset = consumeRequest.getProcessQueue().removeMessage(Collections.singletonList(consumeRequest.getMessageExt()));
            try {
                rocketmqPullConsumer.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
            } catch (MQClientException e) {
                log.error("A error occurred in update consume offset process.", e);
            }
        }
    }

    void ack(final MessageQueue messageQueue, final ProcessQueue processQueue, final MessageExt messageExt) {
        consumedRequest.remove(messageExt.getMsgId());
        long offset = processQueue.removeMessage(Collections.singletonList(messageExt));
        try {
            rocketmqPullConsumer.updateConsumeOffset(messageQueue, offset);
        } catch (MQClientException e) {
            log.error("A error occurred in update consume offset process.", e);
        }
    }

    private void cleanExpireMsg() {
        for (final Map.Entry<MessageQueue, ProcessQueue> next : rocketmqPullConsumer.getDefaultMQPullConsumerImpl()
            .getRebalanceImpl().getProcessQueueTable().entrySet()) {
            ProcessQueue pq = next.getValue();
            MessageQueue mq = next.getKey();
            ReadWriteLock lockTreeMap = getLockInProcessQueue(pq);
            if (lockTreeMap == null) {
                log.error("Gets tree map lock in process queue error, may be has compatibility issue");
                return;
            }

            TreeMap<Long, MessageExt> msgTreeMap = pq.getMsgTreeMap();

            int loop = msgTreeMap.size();
            for (int i = 0; i < loop; i++) {
                MessageExt msg = null;
                try {
                    lockTreeMap.readLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty()) {
                            msg = msgTreeMap.firstEntry().getValue();
                            if (System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msg))
                                > clientConfig.getRmqMessageConsumeTimeout() * 60 * 1000) {
                                //Expired, ack and remove it.
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } finally {
                        lockTreeMap.readLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("Gets expired message exception", e);
                }

                try {
                    rocketmqPullConsumer.sendMessageBack(msg, 3);
                    log.info("Send expired msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}",
                        msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                    ack(mq, pq, msg);
                } catch (Exception e) {
                    log.error("Send back expired msg exception", e);
                }
            }
        }
    }

    private ReadWriteLock getLockInProcessQueue(ProcessQueue pq) {
        try {
            return (ReadWriteLock) FieldUtils.readDeclaredField(pq, "lockTreeMap", true);
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    @Override
    public void start() {
        this.currentState = ServiceLifeState.STARTING;
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanExpireMsg();
            }
        }, clientConfig.getRmqMessageConsumeTimeout(), clientConfig.getRmqMessageConsumeTimeout(), TimeUnit.MINUTES);
        this.currentState = ServiceLifeState.STARTED;
    }

    @Override
    public void stop() {
        this.currentState = ServiceLifeState.STOPPING;
        ThreadUtils.shutdownGracefully(cleanExpireMsgExecutors, 5000, TimeUnit.MILLISECONDS);
        this.currentState = ServiceLifeState.STOPPED;
    }

    @Override
    public ServiceLifeState currentState() {
        return currentState;
    }

    @Override
    public Set<QueueMetaData> getQueueMetaData(String queueName) {
        Set<MessageQueue> messageQueues;
        try {
            messageQueues = rocketmqPullConsumer.fetchSubscribeMessageQueues(queueName);
        } catch (MQClientException e) {
            log.error("A error occurred when get queue metadata.", e);
            return null;
        }
        return OMSClientUtil.queueMetaDataConvert(messageQueues);
    }
}
