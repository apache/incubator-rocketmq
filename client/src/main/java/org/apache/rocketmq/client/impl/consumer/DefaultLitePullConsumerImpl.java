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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class DefaultLitePullConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();

    private final long consumerStartTimestamp = System.currentTimeMillis();

    private final RPCHook rpcHook;

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mQClientFactory;

    private PullAPIWrapper pullAPIWrapper;

    private OffsetStore offsetStore;

    private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private enum SubscriptionType {
        NONE, SUBSCRIBE, ASSIGN
    }

    private static final String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running, please start it first.";

    private static final String SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE = "Subscribe and assign are mutually exclusive.";
    /**
     * the type of subscription
     */
    private SubscriptionType subscriptionType = SubscriptionType.NONE;
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 1000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;

    private DefaultLitePullConsumer defaultLitePullConsumer;

    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private final ScheduledExecutorService scheduledExecutorService;

    private Map<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new HashMap<String, TopicMessageQueueChangeListener>();

    private Map<String, Set<MessageQueue>> messageQueuesForTopic = new HashMap<String, Set<MessageQueue>>();

    private long consumeRequestFlowControlTimes = 0L;

    private long queueFlowControlTimes = 0L;

    private long queueMaxSpanFlowControlTimes = 0L;

    private long nextAutoCommitDeadline = -1L;

    private final MessageQueueLock messageQueueLock = new MessageQueueLock();

    public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, final RPCHook rpcHook) {
        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            this.defaultLitePullConsumer.getPullThreadNums(),
            new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup())
        );
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MonitorMessageQueueChangeThread");
            }
        });
        this.pullTimeDelayMillsWhenException = defaultLitePullConsumer.getPullTimeDelayMillsWhenException();
    }

    private void checkServiceState() {
        if (this.serviceState != ServiceState.RUNNING)
            throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
    }

    public void updateNameServerAddr(String newAddresses) {
        this.mQClientFactory.getMQClientAPIImpl().updateNameServerAddressList(newAddresses);
    }

    private synchronized void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE);
    }

    private void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, assignedMessageQueue);
    }

    private void updatePullTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }
        startPullTask(mqNewSet);
    }

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    updateAssignedMessageQueue(topic, mqAll);
                    updatePullTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    updateAssignedMessageQueue(topic, mqDivided);
                    updatePullTask(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultLitePullConsumer.getConsumerGroup());
                scheduledThreadPoolExecutor.shutdown();
                scheduledExecutorService.shutdown();
                this.mQClientFactory.shutdown();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                log.info("the consumer [{}] shutdown OK", this.defaultLitePullConsumer.getConsumerGroup());
                break;
            default:
                break;
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                if (this.defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultLitePullConsumer.changeInstanceNameToPID();
                }

                initMQClientFactory();

                initRebalanceImpl();

                initPullAPIWrapper();

                initOffsetStore();

                mQClientFactory.start();

                startScheduleTask();

                this.serviceState = ServiceState.RUNNING;

                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());

                operateAfterRunning();

                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
    }

    private void initMQClientFactory() throws MQClientException {
        this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultLitePullConsumer, this.rpcHook);
        boolean registerOK = mQClientFactory.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
        if (!registerOK) {
            this.serviceState = ServiceState.CREATE_JUST;

            throw new MQClientException("The consumer group[" + this.defaultLitePullConsumer.getConsumerGroup()
                + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                null);
        }
    }

    private void initRebalanceImpl() {
        this.rebalanceImpl.setConsumerGroup(this.defaultLitePullConsumer.getConsumerGroup());
        this.rebalanceImpl.setMessageModel(this.defaultLitePullConsumer.getMessageModel());
        this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultLitePullConsumer.getAllocateMessageQueueStrategy());
        this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
    }

    private void initPullAPIWrapper() {
        this.pullAPIWrapper = new PullAPIWrapper(
            mQClientFactory,
            this.defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
        this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
    }

    private void initOffsetStore() throws MQClientException {
        if (this.defaultLitePullConsumer.getOffsetStore() != null) {
            this.offsetStore = this.defaultLitePullConsumer.getOffsetStore();
        } else {
            switch (this.defaultLitePullConsumer.getMessageModel()) {
                case BROADCASTING:
                    this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                case CLUSTERING:
                    this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                default:
                    break;
            }
            this.defaultLitePullConsumer.setOffsetStore(this.offsetStore);
        }
        this.offsetStore.load();
    }

    private void startScheduleTask() {
        scheduledExecutorService.scheduleAtFixedRate(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        fetchTopicMessageQueuesAndCompare();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchMessageQueuesAndCompare exception", e);
                    }
                }
            }, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void operateAfterRunning() throws MQClientException {
        // If subscribe function invoke before start function, then update topic subscribe info after initialization.
        if (subscriptionType == SubscriptionType.SUBSCRIBE) {
            updateTopicSubscribeInfoWhenSubscriptionChanged();
        }
        // If assign function invoke before start function, then update pull task after initialization.
        if (subscriptionType == SubscriptionType.ASSIGN) {
            updateAssignPullTask(assignedMessageQueue.messageQueues());
        }

        for (String topic : topicMessageQueueChangeListenerMap.keySet()) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
        this.mQClientFactory.checkClientInBroker();
    }

    private void checkConfig() throws MQClientException {
        // Check consumerGroup
        Validators.checkGroup(this.defaultLitePullConsumer.getConsumerGroup());

        // Check consumerGroup name is not equal default consumer group name.
        if (this.defaultLitePullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // Check messageModel is not null.
        if (null == this.defaultLitePullConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // Check allocateMessageQueueStrategy is not null
        if (null == this.defaultLitePullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    private void startPullTask(Collection<MessageQueue> mqSet) {
        for (MessageQueue messageQueue : mqSet) {
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void updateAssignPullTask(Collection<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (!mqNewSet.contains(next.getKey())) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }

        startPullTask(mqNewSet);
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            if (topic == null || topic.equals("")) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(defaultLitePullConsumer.getConsumerGroup(),
                topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        try {
            if (topic == null || topic.equals("")) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void unsubscribe(final String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
        removePullTaskCallback(topic);
        assignedMessageQueue.removeAssignedMessageQueue(topic);
    }

    public synchronized void assign(Collection<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.isEmpty()) {
            throw new IllegalArgumentException("Message queues can not be null or empty.");
        }
        setSubscriptionType(SubscriptionType.ASSIGN);
        assignedMessageQueue.updateAssignedMessageQueue(messageQueues);
        if (serviceState == ServiceState.RUNNING) {
            updateAssignPullTask(messageQueues);
        }
    }

    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        if (now >= nextAutoCommitDeadline) {
            commitAll();
            nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
        }
    }

    public synchronized List<MessageExt> poll(long timeout) {
        try {
            checkServiceState();
            if (timeout < 0)
                throw new IllegalArgumentException("Timeout must not be negative");

            if (defaultLitePullConsumer.isAutoCommit()) {
                maybeAutoCommit();
            }
            long endTime = System.currentTimeMillis() + timeout;

            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

            if (endTime - System.currentTimeMillis() > 0) {
                while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
                    consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (endTime - System.currentTimeMillis() <= 0)
                        break;
                }
            }

            if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
                List<MessageExt> messages = consumeRequest.getMessageExts();
                long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                //If namespace not null , reset Topic without namespace.
                this.resetTopic(messages);
                return messages;
            }
        } catch (InterruptedException ignore) {

        }

        return Collections.emptyList();
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.pause(messageQueues);
    }

    public void resume(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.resume(messageQueues);
    }

    public synchronized void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        if (!assignedMessageQueue.messageQueues().contains(messageQueue)) {
            if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                throw new MQClientException("The message queue is not in assigned list, may be rebalancing, message queue: " + messageQueue, null);
            } else {
                throw new MQClientException("The message queue is not in assigned list, message queue: " + messageQueue, null);
            }
        }
        long minOffset = minOffset(messageQueue);
        long maxOffset = maxOffset(messageQueue);
        if (offset < minOffset || offset > maxOffset) {
            throw new MQClientException("Seek offset illegal, seek offset = " + offset + ", min offset = " + minOffset + ", max offset = " + maxOffset, null);
        }
        final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
        synchronized (objLock) {
            assignedMessageQueue.setSeekOffset(messageQueue, offset);
            clearMessageQueueInCache(messageQueue);
        }
    }

    public void seekToBegin(MessageQueue messageQueue) throws MQClientException {
        long begin = minOffset(messageQueue);
        this.seek(messageQueue, begin);
    }

    public void seekToEnd(MessageQueue messageQueue) throws MQClientException {
        long end = maxOffset(messageQueue);
        this.seek(messageQueue, end);
    }

    private long maxOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
    }

    private long minOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().minOffset(messageQueue);
    }

    private void removePullTaskCallback(final String topic) {
        removePullTask(topic);
    }

    private void removePullTask(final String topic) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }
    }

    public synchronized void commitAll() {
        try {
            for (MessageQueue messageQueue : assignedMessageQueue.messageQueues()) {
                long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
                if (consumerOffset != -1) {
                    ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                    if (processQueue != null && !processQueue.isDropped()) {
                        updateConsumeOffset(messageQueue, consumerOffset);
                    }
                }
            }
            if (defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING) {
                offsetStore.persistAll(assignedMessageQueue.messageQueues());
            }
        } catch (Exception e) {
            log.error("An error occurred when update consume offset Automatically.");
        }
    }

    private void updatePullOffset(MessageQueue messageQueue, long nextPullOffset) {
        if (assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
            assignedMessageQueue.updatePullOffset(messageQueue, nextPullOffset);
        }
    }

    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException e) {
            log.error("Submit consumeRequest error", e);
        }
    }

    private long fetchConsumeOffset(MessageQueue messageQueue) {
        checkServiceState();
        long offset = this.rebalanceImpl.computePullFromWhere(messageQueue);
        return offset;
    }

    public long committed(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        long offset = this.offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
        if (offset == -2)
            throw new MQClientException("Fetch consume offset from broker exception", null);
        return offset;
    }

    private void clearMessageQueueInCache(MessageQueue messageQueue) {
        ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
        if (processQueue != null) {
            processQueue.clear();
        }
        Iterator<ConsumeRequest> iter = consumeRequestCache.iterator();
        while (iter.hasNext()) {
            if (iter.next().getMessageQueue().equals(messageQueue))
                iter.remove();
        }
    }

    private long nextPullOffset(MessageQueue messageQueue) {
        long offset = -1;
        long seekOffset = assignedMessageQueue.getSeekOffset(messageQueue);
        if (seekOffset != -1) {
            offset = seekOffset;
            assignedMessageQueue.updateConsumeOffset(messageQueue, offset);
            assignedMessageQueue.setSeekOffset(messageQueue, -1);
        } else {
            offset = assignedMessageQueue.getPullOffset(messageQueue);
            if (offset == -1) {
                offset = fetchConsumeOffset(messageQueue);
            }
        }
        return offset;
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;
        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {

            if (!this.isCancelled()) {

                if (assignedMessageQueue.isPaused(messageQueue)) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }

                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);

                if (processQueue == null && processQueue.isDropped()) {
                    log.info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                    return;
                }

                if (consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchSize() > defaultLitePullConsumer.getPullThresholdForAll()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((consumeRequestFlowControlTimes++ % 1000) == 0)
                        log.warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", consumeRequestCache.size(), consumeRequestFlowControlTimes);
                    return;
                }

                long cachedMessageCount = processQueue.getMsgCount().get();
                long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

                if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }

                if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }

                if (processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), queueMaxSpanFlowControlTimes);
                    }
                    return;
                }

                long offset = nextPullOffset(messageQueue);
                long pullDelayTimeMills = 0;
                try {
                    SubscriptionData subscriptionData;
                    if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                        String topic = this.messageQueue.getTopic();
                        subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                    } else {
                        String topic = this.messageQueue.getTopic();
                        subscriptionData = FilterAPI.buildSubscriptionData(defaultLitePullConsumer.getConsumerGroup(),
                            topic, SubscriptionData.SUB_ALL);
                    }
                    
                    PullResult pullResult = pull(messageQueue, subscriptionData, offset, defaultLitePullConsumer.getPullBatchSize());

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
                            synchronized (objLock) {
                                if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
                                    processQueue.putMessage(pullResult.getMsgFoundList());
                                    submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                                }
                            }
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("The pull request offset illegal, {}", pullResult.toString());
                            break;
                        default:
                            break;
                    }
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset());
                } catch (Throwable e) {
                    pullDelayTimeMills = pullTimeDelayMillsWhenException;
                    log.error("An error occurred in pull message process.", e);
                }

                if (!this.isCancelled()) {
                    scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                }
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
    }

    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums,
        boolean block,
        long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);

        long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
            mq,
            subscriptionData.getSubString(),
            subscriptionData.getExpressionType(),
            isTagType ? 0L : subscriptionData.getSubVersion(),
            offset,
            maxNums,
            sysFlag,
            0,
            this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
            timeoutMillis,
            CommunicationMode.SYNC,
            null
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        return pullResult;
    }

    private void resetTopic(List<MessageExt> msgList) {
        if (null == msgList || msgList.size() == 0) {
            return;
        }

        //If namespace not null , reset Topic without namespace.
        for (MessageExt messageExt : msgList) {
            if (null != this.defaultLitePullConsumer.getNamespace()) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultLitePullConsumer.getNamespace()));
            }
        }

    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        checkServiceState();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    @Override
    public String groupName() {
        return this.defaultLitePullConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultLitePullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            checkServiceState();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            if (this.subscriptionType == SubscriptionType.SUBSCRIBE) {
                Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
                mqs.addAll(allocateMq);
            } else if (this.subscriptionType == SubscriptionType.ASSIGN) {
                Set<MessageQueue> assignedMessageQueue = this.assignedMessageQueue.getAssignedMessageQueues();
                mqs.addAll(assignedMessageQueue);
            }
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("Persist consumer offset error for group: {} ", this.defaultLitePullConsumer.getConsumerGroup(), e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultLitePullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public DefaultLitePullConsumer getDefaultLitePullConsumer() {
        return defaultLitePullConsumer;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        checkServiceState();
        Set<MessageQueue> result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        return parseMessageQueues(result);
    }

    private synchronized void fetchTopicMessageQueuesAndCompare() throws MQClientException {
        for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
            String topic = entry.getKey();
            TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();
            Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
            Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);
            boolean isChanged = !isSetEqual(newMessageQueues, oldMessageQueues);
            if (isChanged) {
                messageQueuesForTopic.put(topic, newMessageQueues);
                if (topicMessageQueueChangeListener != null) {
                    topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
                }
            }
        }
    }

    private boolean isSetEqual(Set<MessageQueue> set1, Set<MessageQueue> set2) {
        if (set1 == null && set2 == null) {
            return true;
        }

        if (set1 == null || set2 == null || set1.size() != set2.size()
            || set1.size() == 0 || set2.size() == 0) {
            return false;
        }

        Iterator iter = set2.iterator();
        boolean isEqual = true;
        while (iter.hasNext()) {
            if (!set1.contains(iter.next())) {
                isEqual = false;
            }
        }
        return isEqual;
    }

    public synchronized void registerTopicMessageQueueChangeListener(String topic,
        TopicMessageQueueChangeListener listener) throws MQClientException {
        if (topic == null || listener == null) {
            throw new MQClientException("Topic or listener is null", null);
        }
        if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
            log.warn("Topic {} had been registered, new listener will overwrite the old one", topic);
        }
        topicMessageQueueChangeListenerMap.put(topic, listener);
        if (this.serviceState == ServiceState.RUNNING) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
    }

    private Set<MessageQueue> parseMessageQueues(Set<MessageQueue> queueSet) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue messageQueue : queueSet) {
            String userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(),
                this.defaultLitePullConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
        }
        return resultQueues;
    }

    public class ConsumeRequest {
        private final List<MessageExt> messageExts;
        private final MessageQueue messageQueue;
        private final ProcessQueue processQueue;

        public ConsumeRequest(final List<MessageExt> messageExts, final MessageQueue messageQueue,
            final ProcessQueue processQueue) {
            this.messageExts = messageExts;
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public List<MessageExt> getMessageExts() {
            return messageExts;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
