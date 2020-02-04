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
import io.openmessaging.Message;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.exception.OMSRuntimeException;
import io.openmessaging.rocketmq.config.ClientConfig;
import io.openmessaging.rocketmq.domain.ConsumeRequest;
import io.openmessaging.rocketmq.utils.BeanUtils;
import io.openmessaging.rocketmq.utils.OMSUtil;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class PullConsumerImpl implements PullConsumer {
    private final DefaultMQPullConsumer rocketmqPullConsumer;
    private final KeyValue properties;
    private boolean started = false;
    private final MQPullConsumerScheduleService pullConsumerScheduleService;
    private final LocalMessageCache localMessageCache;
    private final ClientConfig clientConfig;

    private final static InternalLogger log = ClientLogger.getLog();

    public PullConsumerImpl(final KeyValue properties) {
        this.properties = properties;
        this.clientConfig = BeanUtils.populate(properties, ClientConfig.class);

        String consumerGroup = clientConfig.getConsumerId();
        if (null == consumerGroup || consumerGroup.isEmpty()) {
            throw new OMSRuntimeException("-1", "Consumer Group is necessary for RocketMQ, please set it.");
        }
        pullConsumerScheduleService = new MQPullConsumerScheduleService(consumerGroup);

        this.rocketmqPullConsumer = pullConsumerScheduleService.getDefaultMQPullConsumer();

        if ("true".equalsIgnoreCase(System.getenv("OMS_RMQ_DIRECT_NAME_SRV"))) {
            String accessPoints = clientConfig.getAccessPoints();
            if (accessPoints == null || accessPoints.isEmpty()) {
                throw new OMSRuntimeException("-1", "OMS AccessPoints is null or empty.");
            }
            this.rocketmqPullConsumer.setNamesrvAddr(accessPoints.replace(',', ';'));
        }

        this.rocketmqPullConsumer.setConsumerGroup(consumerGroup);

        int maxReDeliveryTimes = clientConfig.getRmqMaxRedeliveryTimes();
        this.rocketmqPullConsumer.setMaxReconsumeTimes(maxReDeliveryTimes);

        String consumerId = OMSUtil.buildInstanceName();
        this.rocketmqPullConsumer.setInstanceName(consumerId);
        properties.put(OMSBuiltinKeys.CONSUMER_ID, consumerId);

        this.rocketmqPullConsumer.setLanguage(LanguageCode.OMS);

        this.localMessageCache = new LocalMessageCache(this.rocketmqPullConsumer, clientConfig);
    }

    @Override
    public KeyValue attributes() {
        return properties;
    }

    @Override
    public PullConsumer attachQueue(String queueName) {
        registerPullTaskCallback(queueName);
        return this;
    }

    @Override
    public PullConsumer attachQueue(String queueName, KeyValue attributes) {
        registerPullTaskCallback(queueName);
        return this;
    }

    @Override
    public PullConsumer detachQueue(String queueName) {
        this.rocketmqPullConsumer.getRegisterTopics().remove(queueName);
        return this;
    }

    @Override
    public Message receive() {
        MessageExt rmqMsg = localMessageCache.poll();
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public Message receive(final KeyValue properties) {
        MessageExt rmqMsg = localMessageCache.poll(properties);
        return rmqMsg == null ? null : OMSUtil.msgConvert(rmqMsg);
    }

    @Override
    public void ack(final String messageId) {
        localMessageCache.ack(messageId);
    }

    @Override
    public void ack(final String messageId, final KeyValue properties) {
        localMessageCache.ack(messageId);
    }

    @Override
    public synchronized void startup() {
        if (!started) {
            try {
                this.pullConsumerScheduleService.start();
                this.localMessageCache.startup();
            } catch (MQClientException e) {
                throw new OMSRuntimeException("-1", e);
            }
        }
        this.started = true;
    }

    private void registerPullTaskCallback(final String targetQueueName) {
        this.pullConsumerScheduleService.registerPullTaskCallback(targetQueueName, new PullTaskCallback() {
            @Override
            public void doPullTask(final MessageQueue mq, final PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    long offset = localMessageCache.nextPullOffset(mq);

                    PullResult pullResult = consumer.pull(mq, "*",
                        offset, localMessageCache.nextPullBatchNums());
                    ProcessQueue pq = rocketmqPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl()
                        .getProcessQueueTable().get(mq);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            if (pq != null) {
                                pq.putMessage(pullResult.getMsgFoundList());
                                for (final MessageExt messageExt : pullResult.getMsgFoundList()) {
                                    localMessageCache.submitConsumeRequest(new ConsumeRequest(messageExt, mq, pq));
                                }
                            }
                            break;
                        default:
                            break;
                    }
                    localMessageCache.updatePullOffset(mq, pullResult.getNextBeginOffset());
                } catch (Exception e) {
                    log.error("An error occurred in pull message process.", e);
                }
            }
        });
    }

    @Override
    public synchronized void shutdown() {
        if (this.started) {
            this.localMessageCache.shutdown();
            this.pullConsumerScheduleService.shutdown();
            this.rocketmqPullConsumer.shutdown();
        }
        this.started = false;
    }
}
