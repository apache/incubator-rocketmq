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

package org.apache.rocketmq.test.smoke;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendResultForLogicalQueue;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.logicalqueue.MigrateTopicLogicalQueueCommand;
import org.apache.rocketmq.tools.command.logicalqueue.UpdateTopicLogicalQueueMappingCommand;
import org.apache.rocketmq.tools.command.logicalqueue.UpdateTopicLogicalQueueNumCommand;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LogicalQueueIT {
    private static final Logger logger = LoggerFactory.getLogger(LogicalQueueIT.class);
    public static String nsAddr;
    private static String broker1Name;
    private static String broker2Name;
    private static String clusterName;
    private static int brokerNum;
    private final static int QUEUE_NUMBERS = 8;
    private static NamesrvController namesrvController;
    private static BrokerController brokerController1;
    private static BrokerController brokerController2;
    private static Map<String, BrokerController> brokerControllerMap;
    private final static List<Object> mqClients = new ArrayList<>();

    private static DefaultMQProducer producer;
    private static DefaultMQPullConsumer consumer;
    private static DefaultMQAdminExt mqAdminExt;
    private static volatile String topic = null;
    private static final String placeholderTopic = "placeholder";
    private static final int MSG_SENT_TIMES = 3;
    private static final int COMMIT_LOG_FILE_SIZE = 512 * 1024;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        namesrvController = IntegrationTestBase.createAndStartNamesrv();
        nsAddr = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();

        int oldCommitLogSize = IntegrationTestBase.COMMIT_LOG_SIZE;
        IntegrationTestBase.COMMIT_LOG_SIZE = COMMIT_LOG_FILE_SIZE;
        brokerController1 = IntegrationTestBase.createAndStartBroker(nsAddr);
        brokerController2 = IntegrationTestBase.createAndStartBroker(nsAddr);
        IntegrationTestBase.COMMIT_LOG_SIZE = oldCommitLogSize;

        clusterName = brokerController1.getBrokerConfig().getBrokerClusterName();
        broker1Name = brokerController1.getBrokerConfig().getBrokerName();
        broker2Name = brokerController2.getBrokerConfig().getBrokerName();
        brokerNum = 2;
        brokerControllerMap = ImmutableList.of(brokerController1, brokerController2).stream().collect(Collectors.toMap(input -> input.getBrokerConfig().getBrokerName(), Function.identity()));

        BaseConf.waitBrokerRegistered(nsAddr, clusterName);

        producer = new DefaultMQProducer(MQRandomUtils.getRandomConsumerGroup());
        mqClients.add(producer);
        producer.setNamesrvAddr(nsAddr);
        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);
        producer.setSendMsgTimeout(1000);
        producer.start();

        consumer = new DefaultMQPullConsumer(BaseConf.initConsumerGroup());
        mqClients.add(consumer);
        consumer.setNamesrvAddr(nsAddr);
        consumer.setConsumerPullTimeoutMillis(1000);
        consumer.start();

        mqAdminExt = new DefaultMQAdminExt(1000);
        mqClients.add(mqAdminExt);
        mqAdminExt.setNamesrvAddr(nsAddr);
        mqAdminExt.start();

        mqAdminExt.createTopic(clusterName, placeholderTopic, 1);
    }

    @AfterClass
    public static void afterClass() {
        BaseConf.shutdown(mqClients);
        brokerControllerMap.forEach((s, brokerController) -> brokerController.shutdown());
        ofNullable(namesrvController).ifPresent(obj -> ForkJoinPool.commonPool().execute(obj::shutdown));
    }

    @Before
    public void setUp() throws Exception {
        topic = "tt-" + MQRandomUtils.getRandomTopic();
        logger.info("use topic: {}", topic);
        mqAdminExt.createTopic(clusterName, topic, QUEUE_NUMBERS);
        assertThat(mqAdminExt.examineTopicRouteInfo(topic).getBrokerDatas()).hasSize(brokerNum);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !mqAdminExt.examineTopicStats(topic).getOffsetTable().isEmpty());

        consumer.setRegisterTopics(Collections.singleton(topic));
        // consumer.setMessageQueueListener & consumer.registerMessageQueueListener are useless in DefaultMQPullConsumer, they will never work, so do not need to test it

        new UpdateTopicLogicalQueueMappingCommand().execute(mqAdminExt, topic, brokerControllerMap.values().stream().map(BrokerController::getBrokerAddr).collect(Collectors.toSet()));
    }

    private static String getCurrentMethodName() {
        // 0: getStackTrace
        // 1: getCurrentMethodName
        // 2: __realMethod__
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    @Test
    public void test001_SendPullSync() throws Exception {
        String methodName = getCurrentMethodName();

        List<MessageQueue> publishMessageQueues = producer.fetchPublishMessageQueues(topic);
        assertThat(publishMessageQueues).hasSize(brokerNum * QUEUE_NUMBERS);
        Set<Integer> queueIds = IntStream.range(0, brokerNum * QUEUE_NUMBERS).boxed().collect(Collectors.toSet());
        for (MessageQueue messageQueue : publishMessageQueues) {
            assertThat(messageQueue.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
            assertThat(queueIds.remove(messageQueue.getQueueId())).isTrue();
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-sync-%d-%d", methodName, messageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), messageQueue);
                assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(messageQueue.getBrokerName());
                assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(messageQueue.getQueueId());
            }
        }
        assertThat(queueIds).isEmpty();

        List<MessageQueue> subscribeMessageQueues = consumer.fetchSubscribeMessageQueues(topic).stream().sorted().collect(Collectors.toList());
        assertThat(subscribeMessageQueues).hasSize(brokerNum * QUEUE_NUMBERS);
        subscribeMessageQueues.sort(Comparator.comparingInt(MessageQueue::getQueueId));
        queueIds.addAll(IntStream.range(0, brokerNum * QUEUE_NUMBERS).boxed().collect(Collectors.toSet()));
        for (MessageQueue messageQueue : subscribeMessageQueues) {
            assertThat(messageQueue.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
            assertThat(queueIds.remove(messageQueue.getQueueId())).isTrue();
            long offset = mqAdminExt.minOffset(messageQueue);
            PullResult pullResult = consumer.pull(messageQueue, "*", offset, 10);
            assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
            assertThat(pullResult.getMsgFoundList()).hasSize(MSG_SENT_TIMES);
            offset = -1;
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                MessageExt msg = pullResult.getMsgFoundList().get(i);
                assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
                assertThat(msg.getQueueId()).isEqualTo(messageQueue.getQueueId());
                assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-sync-%d-%d", methodName, messageQueue.getQueueId(), i));
                if (i > 0) {
                    assertThat(msg.getQueueOffset()).isEqualTo(offset + i);
                } else {
                    offset = msg.getQueueOffset();
                }
            }
            assertThat(maxOffsetUncommitted(messageQueue)).isEqualTo(offset + MSG_SENT_TIMES);
        }
        assertThat(queueIds).isEmpty();
    }

    @Test
    public void test002_SendPullAsync() throws Exception {
        String methodName = getCurrentMethodName();

        List<MessageQueue> publishMessageQueues = producer.fetchPublishMessageQueues(topic);
        for (MessageQueue messageQueue : publishMessageQueues) {
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                CompletableFuture<SendResult> future = new CompletableFuture<>();
                producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-async-%d-%d", methodName, messageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), messageQueue, new SendCallback() {
                    @Override public void onSuccess(SendResult sendResult) {
                        future.complete(sendResult);
                    }

                    @Override public void onException(Throwable e) {
                        future.completeExceptionally(e);
                    }
                });
                SendResult sendResult = future.get();
                assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(messageQueue.getBrokerName());
                assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(messageQueue.getQueueId());
            }
        }

        List<MessageQueue> subscribeMessageQueues = consumer.fetchSubscribeMessageQueues(topic).stream().sorted().collect(Collectors.toList());
        for (MessageQueue messageQueue : subscribeMessageQueues) {
            long offset = mqAdminExt.minOffset(messageQueue);
            CompletableFuture<PullResult> future = new CompletableFuture<>();
            consumer.pull(messageQueue, "*", offset, 10, new PullCallback() {
                @Override public void onSuccess(PullResult pullResult) {
                    future.complete(pullResult);
                }

                @Override public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
            PullResult pullResult = future.get();
            assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
            assertThat(pullResult.getMsgFoundList()).hasSize(MSG_SENT_TIMES);
            offset = -1;
            Iterator<MessageExt> it = pullResult.getMsgFoundList().iterator();
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                MessageExt msg = it.next();
                assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
                assertThat(msg.getQueueId()).isEqualTo(messageQueue.getQueueId());
                assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-async-%d-%d", methodName, messageQueue.getQueueId(), i));
                if (i > 0) {
                    assertThat(msg.getQueueOffset()).isEqualTo(offset + i);
                } else {
                    offset = msg.getQueueOffset();
                }
            }
        }
    }

    @Test
    public void test003_MigrateOnceWithoutData() throws Exception {
        final String methodName = getCurrentMethodName();

        final int logicalQueueIdx = 1;

        TopicRouteData topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
        List<LogicalQueueRouteData> logicalQueueRouteDataList1 = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
        LogicalQueueRouteData lastLogicalQueueRouteData1 = logicalQueueRouteDataList1.get(logicalQueueRouteDataList1.size() - 1);
        String newBrokerName;
        if (lastLogicalQueueRouteData1.getBrokerName().equals(broker1Name)) {
            newBrokerName = broker2Name;
        } else {
            newBrokerName = broker1Name;
        }

        MessageQueue migratedMessageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, logicalQueueIdx);

        new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, newBrokerName, null);

        topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
        assertThat(topicRouteInfo.getLogicalQueuesInfo()).isNotNull();
        for (Map.Entry<Integer, List<LogicalQueueRouteData>> entry : topicRouteInfo.getLogicalQueuesInfo().entrySet()) {
            List<LogicalQueueRouteData> logicalQueueRouteDataList2 = entry.getValue();
            if (entry.getKey() == logicalQueueIdx) {
                assertThat(logicalQueueRouteDataList2).hasSize(logicalQueueRouteDataList1.size() + 1);
                LogicalQueueRouteData lastLogicalQueueRouteData2 = logicalQueueRouteDataList2.get(logicalQueueRouteDataList2.size() - 2);
                assertThat(lastLogicalQueueRouteData2.getMessageQueue()).isEqualTo(lastLogicalQueueRouteData1.getMessageQueue());
                assertThat(lastLogicalQueueRouteData2.getOffsetMax()).isGreaterThanOrEqualTo(0L);
                assertThat(lastLogicalQueueRouteData2.getMessagesCount()).isEqualTo(0L);
                assertThat(lastLogicalQueueRouteData2.isWritable()).isFalse();
                assertThat(lastLogicalQueueRouteData2.isReadable()).isFalse();
                assertThat(lastLogicalQueueRouteData2.isExpired()).isTrue();
                assertThat(lastLogicalQueueRouteData2.getLogicalQueueDelta()).isEqualTo(0L);

                LogicalQueueRouteData lastLogicalQueueRouteData3 = logicalQueueRouteDataList2.get(logicalQueueRouteDataList2.size() - 1);
                assertThat(lastLogicalQueueRouteData3.getBrokerName()).isEqualTo(newBrokerName);
                assertThat(lastLogicalQueueRouteData3.getOffsetMax()).isLessThan(0L);
                assertThat(lastLogicalQueueRouteData3.isWritable()).isTrue();
                assertThat(lastLogicalQueueRouteData3.isReadable()).isTrue();
                assertThat(lastLogicalQueueRouteData3.isExpired()).isFalse();
                assertThat(lastLogicalQueueRouteData3.getLogicalQueueDelta()).isEqualTo(0L);
            } else {
                assertThat(logicalQueueRouteDataList2).hasSize(1);
                LogicalQueueRouteData logicalQueueRouteData = logicalQueueRouteDataList2.get(0);
                assertThat(logicalQueueRouteData.getOffsetMax()).isLessThan(0L);
                assertThat(logicalQueueRouteData.isWritable()).isTrue();
                assertThat(logicalQueueRouteData.isReadable()).isTrue();
                assertThat(logicalQueueRouteData.isExpired()).isFalse();
                assertThat(logicalQueueRouteData.getLogicalQueueDelta()).isEqualTo(0L);
            }
        }

        List<MessageQueue> subscribeMessageQueues = consumer.fetchSubscribeMessageQueues(topic).stream().sorted().collect(Collectors.toList());
        assertThat(subscribeMessageQueues).hasSize(brokerNum * QUEUE_NUMBERS);
        for (MessageQueue mq : subscribeMessageQueues) {
            assertThat(mqAdminExt.minOffset(mq)).isEqualTo(0L);
        }

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-sync-%d-%d", methodName, migratedMessageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue);
            assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(migratedMessageQueue.getBrokerName());
            assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(newBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(QUEUE_NUMBERS);
        }

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            CompletableFuture<SendResult> future = new CompletableFuture<>();
            producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-async-%d-%d", methodName, migratedMessageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue, new SendCallback() {
                @Override public void onSuccess(SendResult sendResult) {
                    future.complete(sendResult);
                }

                @Override public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
            SendResult sendResult = future.get();
            assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(migratedMessageQueue.getBrokerName());
            assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(newBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(QUEUE_NUMBERS);
        }

        assertThat(maxOffsetUncommitted(migratedMessageQueue)).isEqualTo(2 * MSG_SENT_TIMES);

        waitAtMost(5, TimeUnit.SECONDS).until(() -> mqAdminExt.maxOffset(migratedMessageQueue) == 2 * MSG_SENT_TIMES);

        PullResult pullResult = consumer.pull(migratedMessageQueue, "*", 0L, 2 * MSG_SENT_TIMES);
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
        assertThat(pullResult.getMinOffset()).isEqualTo(0);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
        assertThat(msgFoundList).hasSize(2 * MSG_SENT_TIMES);
        Iterator<MessageExt> it = pullResult.getMsgFoundList().iterator();
        long offset = 0L;
        for (String prefix : new String[] {"sync", "async"}) {
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                MessageExt msg = it.next();
                assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
                assertThat(msg.getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
                assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-%s-%d-%d", methodName, prefix, migratedMessageQueue.getQueueId(), i));
                assertThat(msg.getQueueOffset()).isEqualTo(offset);
                offset++;
            }
        }

        offset = pullResult.getNextBeginOffset();
        CompletableFuture<PullResult> future = new CompletableFuture<>();
        consumer.pull(migratedMessageQueue, "*", offset, 10, new PullCallback() {
            @Override public void onSuccess(PullResult pullResult) {
                future.complete(pullResult);
            }

            @Override public void onException(Throwable e) {
                future.completeExceptionally(e);
            }
        });
        pullResult = future.get();
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.NO_NEW_MSG);
        assertThat(pullResult.getMinOffset()).isEqualTo(0);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        assertThat(pullResult.getMsgFoundList()).isNull();
    }

    @Test
    public void test004_MigrateOnceWithData() throws Exception {
        final String methodName = getCurrentMethodName();

        final int logicalQueueIdx = 1;

        TopicRouteData topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
        List<LogicalQueueRouteData> logicalQueueRouteDataList1 = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
        LogicalQueueRouteData lastLogicalQueueRouteData1 = logicalQueueRouteDataList1.get(logicalQueueRouteDataList1.size() - 1);
        String newBrokerName;
        if (lastLogicalQueueRouteData1.getBrokerName().equals(broker1Name)) {
            newBrokerName = broker2Name;
        } else {
            newBrokerName = broker1Name;
        }

        MessageQueue migratedMessageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, logicalQueueIdx);

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-sync-%d-%d", methodName, migratedMessageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue);
            assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(migratedMessageQueue.getBrokerName());
            assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
        }
        assertThat(maxOffsetUncommitted(migratedMessageQueue)).isEqualTo(MSG_SENT_TIMES);

        waitAtMost(5, TimeUnit.SECONDS).until(() -> mqAdminExt.maxOffset(migratedMessageQueue) == MSG_SENT_TIMES);

        {
            long offset = 0L;
            PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 2 * MSG_SENT_TIMES);
            assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
            assertThat(pullResult.getMinOffset()).isEqualTo(0);
            assertThat(pullResult.getMaxOffset()).isEqualTo(MSG_SENT_TIMES);
            assertThat(pullResult.getNextBeginOffset()).isEqualTo(MSG_SENT_TIMES);
            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
            assertThat(msgFoundList).hasSize(MSG_SENT_TIMES);
            Iterator<MessageExt> it = pullResult.getMsgFoundList().iterator();
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                MessageExt msg = it.next();
                assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
                assertThat(msg.getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
                assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-sync-%d-%d", methodName, migratedMessageQueue.getQueueId(), i));
                assertThat(msg.getQueueOffset()).isEqualTo(offset);
                offset++;
            }
        }

        new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, newBrokerName, null);

        topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
        assertThat(topicRouteInfo.getLogicalQueuesInfo()).isNotNull();
        for (Map.Entry<Integer, List<LogicalQueueRouteData>> entry : topicRouteInfo.getLogicalQueuesInfo().entrySet()) {
            List<LogicalQueueRouteData> logicalQueueRouteDataList2 = entry.getValue();
            if (entry.getKey() == logicalQueueIdx) {
                assertThat(logicalQueueRouteDataList2).hasSize(logicalQueueRouteDataList1.size() + 1);
                LogicalQueueRouteData lastLogicalQueueRouteData2 = logicalQueueRouteDataList2.get(logicalQueueRouteDataList2.size() - 2);
                assertThat(lastLogicalQueueRouteData2.getMessageQueue()).isEqualTo(lastLogicalQueueRouteData1.getMessageQueue());
                assertThat(lastLogicalQueueRouteData2.getOffsetMax()).isGreaterThanOrEqualTo(0L);
                assertThat(lastLogicalQueueRouteData2.getMessagesCount()).isEqualTo(MSG_SENT_TIMES);
                assertThat(lastLogicalQueueRouteData2.isWritable()).isFalse();
                assertThat(lastLogicalQueueRouteData2.isReadable()).isTrue();
                assertThat(lastLogicalQueueRouteData2.isExpired()).isFalse();
                assertThat(lastLogicalQueueRouteData2.getLogicalQueueDelta()).isEqualTo(0L);

                LogicalQueueRouteData lastLogicalQueueRouteData3 = logicalQueueRouteDataList2.get(logicalQueueRouteDataList2.size() - 1);
                assertThat(lastLogicalQueueRouteData3.getBrokerName()).isEqualTo(newBrokerName);
                assertThat(lastLogicalQueueRouteData3.getOffsetMax()).isLessThan(0L);
                assertThat(lastLogicalQueueRouteData3.isWritable()).isTrue();
                assertThat(lastLogicalQueueRouteData3.isReadable()).isTrue();
                assertThat(lastLogicalQueueRouteData3.isExpired()).isFalse();
                assertThat(lastLogicalQueueRouteData3.getLogicalQueueDelta()).isEqualTo(MSG_SENT_TIMES);
            } else {
                assertThat(logicalQueueRouteDataList2).hasSize(1);
                LogicalQueueRouteData logicalQueueRouteData = logicalQueueRouteDataList2.get(0);
                assertThat(logicalQueueRouteData.getOffsetMax()).isLessThan(0L);
                assertThat(logicalQueueRouteData.isWritable()).isTrue();
                assertThat(logicalQueueRouteData.isReadable()).isTrue();
                assertThat(logicalQueueRouteData.isExpired()).isFalse();
                assertThat(logicalQueueRouteData.getLogicalQueueDelta()).isEqualTo(0L);
            }
        }
        assertThat(migratedMessageQueue).isNotNull();

        List<MessageQueue> subscribeMessageQueues = consumer.fetchSubscribeMessageQueues(topic).stream().sorted().collect(Collectors.toList());
        assertThat(subscribeMessageQueues).hasSize(brokerNum * QUEUE_NUMBERS);
        for (MessageQueue mq : subscribeMessageQueues) {
            assertThat(mqAdminExt.minOffset(mq)).isEqualTo(0L);
        }

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            CompletableFuture<SendResult> future = new CompletableFuture<>();
            producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-async-%d-%d", methodName, migratedMessageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue, new SendCallback() {
                @Override public void onSuccess(SendResult sendResult) {
                    future.complete(sendResult);
                }

                @Override public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            });
            SendResult sendResult = future.get();
            assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(migratedMessageQueue.getBrokerName());
            assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(newBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(QUEUE_NUMBERS);
        }

        assertThat(maxOffsetUncommitted(migratedMessageQueue)).isEqualTo(2 * MSG_SENT_TIMES);

        waitAtMost(5, TimeUnit.SECONDS).until(() -> mqAdminExt.maxOffset(migratedMessageQueue) == 2 * MSG_SENT_TIMES);

        long offset = 0L;
        PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 2 * MSG_SENT_TIMES);
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
        assertThat(pullResult.getMinOffset()).isEqualTo(0);
        assertThat(pullResult.getMaxOffset()).isEqualTo(MSG_SENT_TIMES);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(MSG_SENT_TIMES);
        List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
        assertThat(msgFoundList).hasSize(MSG_SENT_TIMES);
        Iterator<MessageExt> it = pullResult.getMsgFoundList().iterator();
        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            MessageExt msg = it.next();
            assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
            assertThat(msg.getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
            assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-sync-%d-%d", methodName, migratedMessageQueue.getQueueId(), i));
            assertThat(msg.getQueueOffset()).isEqualTo(offset);
            offset++;
        }

        offset = pullResult.getNextBeginOffset();
        CompletableFuture<PullResult> pullResultFuture = new CompletableFuture<>();
        consumer.pull(migratedMessageQueue, "*", offset, 2 * MSG_SENT_TIMES, new PullCallback() {
            @Override public void onSuccess(PullResult pullResult) {
                pullResultFuture.complete(pullResult);
            }

            @Override public void onException(Throwable e) {
                pullResultFuture.completeExceptionally(e);
            }
        });
        pullResult = pullResultFuture.get();
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
        assertThat(pullResult.getMinOffset()).isEqualTo(MSG_SENT_TIMES);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        msgFoundList = pullResult.getMsgFoundList();
        assertThat(msgFoundList).hasSize(MSG_SENT_TIMES);
        it = pullResult.getMsgFoundList().iterator();
        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            MessageExt msg = it.next();
            assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
            assertThat(msg.getQueueId()).isEqualTo(migratedMessageQueue.getQueueId());
            assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-async-%d-%d", methodName, migratedMessageQueue.getQueueId(), i));
            assertThat(msg.getQueueOffset()).isEqualTo(offset);
            offset++;
        }

        offset = pullResult.getNextBeginOffset();
        pullResult = consumer.pull(migratedMessageQueue, "*", offset, 10);
        assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.NO_NEW_MSG);
        assertThat(pullResult.getMinOffset()).isEqualTo(MSG_SENT_TIMES);
        assertThat(pullResult.getMaxOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        assertThat(pullResult.getNextBeginOffset()).isEqualTo(2 * MSG_SENT_TIMES);
        assertThat(pullResult.getMsgFoundList()).isNull();
    }

    @Test
    public void test005_MigrateWithDataBackAndForth() throws Exception {
        final String methodName = getCurrentMethodName();

        final int logicalQueueIdx = 1;

        MessageQueue migratedMessageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, logicalQueueIdx);

        BrokerController brokerController;

        TopicRouteData topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
        LogicalQueueRouteData lastLogicalQueueRouteData;
        {
            List<LogicalQueueRouteData> logicalQueueRouteDataList = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
            lastLogicalQueueRouteData = logicalQueueRouteDataList.get(logicalQueueRouteDataList.size() - 1);
        }
        final String fromBrokerName, toBrokerName, fromBrokerAddr, toBrokerAddr;
        if (lastLogicalQueueRouteData.getBrokerName().equals(broker1Name)) {
            fromBrokerName = broker1Name;
            fromBrokerAddr = brokerController1.getBrokerAddr();
            toBrokerName = broker2Name;
            toBrokerAddr = brokerController2.getBrokerAddr();
        } else {
            fromBrokerName = broker2Name;
            fromBrokerAddr = brokerController2.getBrokerAddr();
            toBrokerName = broker1Name;
            toBrokerAddr = brokerController1.getBrokerAddr();
        }

        int msgIdx = 0;

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, msgIdx++).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue);
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(fromBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(logicalQueueIdx);
        }

        rotateBrokerCommitLog(brokerControllerMap.get(fromBrokerName));

        new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, toBrokerName, null);

        {
            LogicalQueuesInfo info;
            List<LogicalQueueRouteData> logicalQueueRouteDataList;
            info = mqAdminExt.queryTopicLogicalQueueMapping(fromBrokerAddr, topic);
            logicalQueueRouteDataList = info.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).hasSize(2);
            info = mqAdminExt.queryTopicLogicalQueueMapping(toBrokerAddr, topic);
            logicalQueueRouteDataList = info.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).hasSize(1);
        }

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, msgIdx++).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue);
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(toBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(QUEUE_NUMBERS);
        }

        new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, fromBrokerName, null);
        // now will reuse queue with a ReadOnly one

        {
            LogicalQueuesInfo info;
            List<LogicalQueueRouteData> logicalQueueRouteDataList;
            info = mqAdminExt.queryTopicLogicalQueueMapping(fromBrokerAddr, topic);
            logicalQueueRouteDataList = info.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).hasSize(3);
            info = mqAdminExt.queryTopicLogicalQueueMapping(toBrokerAddr, topic);
            logicalQueueRouteDataList = info.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).hasSize(2);
        }

        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, msgIdx++).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue);
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(fromBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(logicalQueueIdx);
        }

        LogicalQueueRouteData logicalQueueRouteData1;
        LogicalQueueRouteData logicalQueueRouteData2;
        {
            List<LogicalQueueRouteData> logicalQueueRouteDataList;
            topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
            logicalQueueRouteDataList = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).hasSize(3);
            logicalQueueRouteData1 = logicalQueueRouteDataList.get(0);
            assertThat(logicalQueueRouteData1.getLogicalQueueDelta()).isEqualTo(0);
            assertThat(logicalQueueRouteData1.isReadable()).isTrue();
            assertThat(logicalQueueRouteData1.isWritable()).isFalse();
            assertThat(logicalQueueRouteData1.isExpired()).isFalse();
            assertThat(logicalQueueRouteData1.isWriteOnly()).isFalse();
            assertThat(logicalQueueRouteData1.getBrokerName()).isEqualTo(fromBrokerName);
            assertThat(logicalQueueRouteData1.getOffsetMax()).isGreaterThanOrEqualTo(0L);
            assertThat(logicalQueueRouteData1.getMessagesCount()).isEqualTo(MSG_SENT_TIMES);
            assertThat(logicalQueueRouteData1.getFirstMsgTimeMillis()).isGreaterThan(0L);
            assertThat(logicalQueueRouteData1.getLastMsgTimeMillis()).isGreaterThan(0L);
            logicalQueueRouteData2 = logicalQueueRouteDataList.get(1);
            assertThat(logicalQueueRouteData2.getLogicalQueueDelta()).isEqualTo(MSG_SENT_TIMES);
            assertThat(logicalQueueRouteData2.isReadable()).isTrue();
            assertThat(logicalQueueRouteData2.isWritable()).isFalse();
            assertThat(logicalQueueRouteData2.isExpired()).isFalse();
            assertThat(logicalQueueRouteData2.isWriteOnly()).isFalse();
            assertThat(logicalQueueRouteData2.getBrokerName()).isEqualTo(toBrokerName);
            assertThat(logicalQueueRouteData2.getOffsetMax()).isGreaterThanOrEqualTo(0L);
            assertThat(logicalQueueRouteData2.getMessagesCount()).isEqualTo(MSG_SENT_TIMES);
            assertThat(logicalQueueRouteData2.getFirstMsgTimeMillis()).isGreaterThan(0L);
            assertThat(logicalQueueRouteData2.getLastMsgTimeMillis()).isGreaterThan(0L);
            LogicalQueueRouteData logicalQueueRouteData3 = logicalQueueRouteDataList.get(2);
            assertThat(logicalQueueRouteData3.getLogicalQueueDelta()).isEqualTo(2 * MSG_SENT_TIMES);
            assertThat(logicalQueueRouteData3.isReadable()).isTrue();
            assertThat(logicalQueueRouteData3.isWritable()).isTrue();
            assertThat(logicalQueueRouteData3.isExpired()).isFalse();
            assertThat(logicalQueueRouteData3.isWriteOnly()).isFalse();
            assertThat(logicalQueueRouteData3.getBrokerName()).isEqualTo(fromBrokerName);
            assertThat(logicalQueueRouteData3.getOffsetMax()).isLessThan(0L);
        }

        msgIdx = 0;
        forLoop:
        for (long offset = 0L; ; ) {
            PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 3 * MSG_SENT_TIMES);
            switch (pullResult.getPullStatus()) {
                case NO_NEW_MSG:
                    assertThat(offset).isGreaterThanOrEqualTo(3L * MSG_SENT_TIMES);
                    break forLoop;
                case OFFSET_ILLEGAL:
                    offset = pullResult.getNextBeginOffset();
                    break;
                default:
                    assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
                    assertThat(pullResult.getMsgFoundList()).isNotNull();
                    assertThat(pullResult.getMsgFoundList()).hasSize(MSG_SENT_TIMES);
                    for (MessageExt msg : pullResult.getMsgFoundList()) {
                        assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, msgIdx));
                        msgIdx++;
                        assertThat(msg.getQueueOffset()).isEqualTo(offset);
                        offset++;
                    }
                    offset = pullResult.getNextBeginOffset();
                    break;
            }
        }

        waitAtMost(5, TimeUnit.SECONDS).until(() -> maxOffsetUncommitted(logicalQueueRouteData1.getMessageQueue()) == mqAdminExt.maxOffset(logicalQueueRouteData1.getMessageQueue()));
        waitAtMost(5, TimeUnit.SECONDS).until(() -> maxOffsetUncommitted(logicalQueueRouteData2.getMessageQueue()) == mqAdminExt.maxOffset(logicalQueueRouteData2.getMessageQueue()));

        // now verify after commit log cleaned, toBroker's first queue route data will be expired too
        brokerController = brokerControllerMap.get(logicalQueueRouteData2.getBrokerName());
        rotateBrokerCommitLog(brokerController);
        deleteCommitLogFiles(brokerController, 1);

        {
            topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
            List<LogicalQueueRouteData> logicalQueueRouteDataList = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).hasSize(2);
            assertThat(logicalQueueRouteDataList.get(0)).isEqualToIgnoringGivenFields(new LogicalQueueRouteData(logicalQueueIdx, 0,  new MessageQueue(topic, fromBrokerName, logicalQueueIdx), MessageQueueRouteState.ReadOnly, 0, 3, -1, -1, fromBrokerAddr), "firstMsgTimeMillis", "lastMsgTimeMillis");
            assertThat(logicalQueueRouteDataList.get(1)).isEqualToComparingFieldByField(new LogicalQueueRouteData(logicalQueueIdx, 2 * MSG_SENT_TIMES, new MessageQueue(topic, fromBrokerName, logicalQueueIdx), MessageQueueRouteState.Normal, MSG_SENT_TIMES, -1, -1, -1, fromBrokerAddr));
        }

        // try pull again, since there is an expired queue route in the middle.
        {
            int msgCount = 0;
            Queue<Integer> wantMsgIdx = new LinkedList<>();
            wantMsgIdx.addAll(IntStream.range(0, MSG_SENT_TIMES).boxed().collect(Collectors.toList()));
            wantMsgIdx.addAll(IntStream.range(2 * MSG_SENT_TIMES, 3 * MSG_SENT_TIMES).boxed().collect(Collectors.toList()));
            forLoop:
            for (long offset = mqAdminExt.minOffset(migratedMessageQueue); ; ) {
                PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 3 * MSG_SENT_TIMES);
                switch (pullResult.getPullStatus()) {
                    case NO_NEW_MSG:
                        assertThat(msgCount).as("offset=%d", offset).isEqualTo(2 * MSG_SENT_TIMES);
                        break forLoop;
                    case OFFSET_ILLEGAL:
                        offset = pullResult.getNextBeginOffset();
                        break;
                    case FOUND:
                        msgCount += pullResult.getMsgFoundList().size();
                        boolean first = true;
                        for (MessageExt msg : pullResult.getMsgFoundList()) {
                            assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).as("offset=%d", offset).isEqualTo(String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, wantMsgIdx.poll()));
                            if (first) {
                                assertThat(msg.getQueueOffset()).isGreaterThanOrEqualTo(offset);
                                first = false;
                            } else {
                                assertThat(msg.getQueueOffset()).isGreaterThan(offset);
                            }
                            offset = msg.getQueueOffset();
                        }
                        offset = pullResult.getNextBeginOffset();
                        break;
                    default:
                        Assert.fail(String.format(Locale.ENGLISH, "unexpected pull offset=%d status: %s", offset, pullResult));
                }
            }
        }

        // rotate first queue route to expired, and pull it
        brokerController = brokerControllerMap.get(logicalQueueRouteData1.getBrokerName());
        rotateBrokerCommitLog(brokerController);
        deleteCommitLogFiles(brokerController, 2);

        {
            List<LogicalQueueRouteData> logicalQueueRouteDataList;
            topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
            logicalQueueRouteDataList = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).isEqualTo(Collections.singletonList(new LogicalQueueRouteData(logicalQueueIdx, 2 * MSG_SENT_TIMES, new MessageQueue(topic, fromBrokerName, logicalQueueIdx), MessageQueueRouteState.Normal, MSG_SENT_TIMES, -1, -1, -1, fromBrokerAddr)));
        }

        {
            int msgCount = 0;
            Queue<Integer> wantMsgIdx = new LinkedList<>();
            wantMsgIdx.addAll(IntStream.range(2 * MSG_SENT_TIMES, 3 * MSG_SENT_TIMES).boxed().collect(Collectors.toList()));
            forLoop:
            for (long offset = mqAdminExt.minOffset(migratedMessageQueue); ; ) {
                PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 3 * MSG_SENT_TIMES);
                switch (pullResult.getPullStatus()) {
                    case NO_NEW_MSG:
                        if (msgCount != MSG_SENT_TIMES) {
                            Assert.fail(String.format(Locale.ENGLISH, "want %d msg but got %d", MSG_SENT_TIMES, msgCount));
                        }
                        break forLoop;
                    case OFFSET_ILLEGAL:
                        offset = pullResult.getNextBeginOffset();
                        break;
                    case FOUND:
                        msgCount += pullResult.getMsgFoundList().size();
                        boolean first = true;
                        for (MessageExt msg : pullResult.getMsgFoundList()) {
                            assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).as("offset=%d", offset).isEqualTo(String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, wantMsgIdx.poll()));
                            if (first) {
                                assertThat(msg.getQueueOffset()).isGreaterThanOrEqualTo(offset);
                                first = false;
                            } else {
                                assertThat(msg.getQueueOffset()).isGreaterThan(offset);
                            }
                            offset = msg.getQueueOffset();
                        }
                        offset = pullResult.getNextBeginOffset();
                        break;
                    default:
                        Assert.fail(String.format(Locale.ENGLISH, "unexpected pull offset=%d status: %s", offset, pullResult));
                }
            }
        }

        brokerController = brokerControllerMap.get(fromBrokerName);
        rotateBrokerCommitLog(brokerController);
        deleteCommitLogFiles(brokerController, 1);

        {
            forLoop:
            for (long offset = mqAdminExt.minOffset(migratedMessageQueue); ; ) {
                PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 3 * MSG_SENT_TIMES);
                // commit log rotate and cleaned, so there is no message.
                switch (pullResult.getPullStatus()) {
                    case NO_MATCHED_MSG:
                    case NO_NEW_MSG:
                        assertThat(pullResult.getNextBeginOffset()).isEqualTo(3 * MSG_SENT_TIMES);
                        break forLoop;
                    case OFFSET_ILLEGAL:
                        offset = pullResult.getNextBeginOffset();
                        break;
                    default:
                        Assert.fail(String.format(Locale.ENGLISH, "unexpected pull offset=%d status: %s", offset, pullResult));
                }
            }
        }

        {
            LogicalQueuesInfo logicalQueuesInfo = mqAdminExt.queryTopicLogicalQueueMapping(brokerController.getBrokerAddr(), topic);
            List<LogicalQueueRouteData> logicalQueueRouteDataList = logicalQueuesInfo.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).isEqualTo(Collections.singletonList(new LogicalQueueRouteData(logicalQueueIdx, 2 * MSG_SENT_TIMES, new MessageQueue(topic, fromBrokerName, logicalQueueIdx), MessageQueueRouteState.Normal, MSG_SENT_TIMES, -1, -1, -1, fromBrokerAddr)));
        }

        // try migrate to this broker which has a expired queue, expect it will reuse the expired one, pull it to verify if delta works well
        new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, toBrokerName, null);

        {
            List<LogicalQueueRouteData> logicalQueueRouteDataList;
            topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
            logicalQueueRouteDataList = topicRouteInfo.getLogicalQueuesInfo().get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).isEqualTo(Arrays.asList(
                new LogicalQueueRouteData(logicalQueueIdx, 2 * MSG_SENT_TIMES, new MessageQueue(topic, fromBrokerName, logicalQueueIdx), MessageQueueRouteState.Expired, MSG_SENT_TIMES, 2 * MSG_SENT_TIMES, 0, 0, fromBrokerAddr)
                , new LogicalQueueRouteData(logicalQueueIdx, 3 * MSG_SENT_TIMES, new MessageQueue(topic, toBrokerName, QUEUE_NUMBERS), MessageQueueRouteState.Normal, MSG_SENT_TIMES, -1, -1, -1, toBrokerAddr)
                ));

            LogicalQueuesInfo info;
            info = mqAdminExt.queryTopicLogicalQueueMapping(fromBrokerAddr, topic);
            logicalQueueRouteDataList = info.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).isEqualTo(Arrays.asList(
                new LogicalQueueRouteData(logicalQueueIdx, 2 * MSG_SENT_TIMES, new MessageQueue(topic, fromBrokerName, logicalQueueIdx), MessageQueueRouteState.Expired, MSG_SENT_TIMES, 2 * MSG_SENT_TIMES, 0, 0, fromBrokerAddr)
                , new LogicalQueueRouteData(logicalQueueIdx, 3 * MSG_SENT_TIMES, new MessageQueue(topic, toBrokerName, QUEUE_NUMBERS), MessageQueueRouteState.Normal, MSG_SENT_TIMES, -1, -1, -1, toBrokerAddr)
            ));
            info = mqAdminExt.queryTopicLogicalQueueMapping(toBrokerAddr, topic);
            logicalQueueRouteDataList = info.get(logicalQueueIdx);
            assertThat(logicalQueueRouteDataList).isEqualTo(Collections.singletonList(new LogicalQueueRouteData(logicalQueueIdx, 3 * MSG_SENT_TIMES, new MessageQueue(topic, toBrokerName, QUEUE_NUMBERS), MessageQueueRouteState.Normal, MSG_SENT_TIMES, -1, -1, -1, toBrokerAddr)));
        }

        msgIdx = 3 * MSG_SENT_TIMES;
        for (int i = 0; i < MSG_SENT_TIMES; i++) {
            SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, msgIdx++).getBytes(StandardCharsets.UTF_8)), migratedMessageQueue);
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(toBrokerName);
            assertThat(sendResult2.getOrigQueueId()).isEqualTo(QUEUE_NUMBERS);
        }

        {
            int msgCount = 0;
            Queue<Integer> wantMsgIdx = new LinkedList<>();
            wantMsgIdx.addAll(IntStream.range(3 * MSG_SENT_TIMES, 4 * MSG_SENT_TIMES).boxed().collect(Collectors.toList()));
            LOOP:
            for (long offset = 0L; ; ) {
                PullResult pullResult = consumer.pull(migratedMessageQueue, "*", offset, 3 * MSG_SENT_TIMES);
                switch (pullResult.getPullStatus()) {
                    case NO_NEW_MSG:
                        assertThat(msgCount).as("msgCount with offset=%d", offset).isEqualTo(MSG_SENT_TIMES);
                        break LOOP;
                    case OFFSET_ILLEGAL:
                        assertThat(pullResult.getNextBeginOffset()).isNotEqualTo(Long.MIN_VALUE);
                        offset = pullResult.getNextBeginOffset();
                        break;
                    case FOUND:
                        msgCount += pullResult.getMsgFoundList().size();
                        boolean first = true;
                        for (MessageExt msg : pullResult.getMsgFoundList()) {
                            assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).as("offset=%d", offset).isEqualTo(String.format(Locale.ENGLISH, "%s-%d-%d", methodName, logicalQueueIdx, wantMsgIdx.poll()));
                            if (first) {
                                assertThat(msg.getQueueOffset()).isGreaterThanOrEqualTo(offset);
                                first = false;
                            } else {
                                assertThat(msg.getQueueOffset()).isGreaterThan(offset);
                            }
                            offset = msg.getQueueOffset();
                        }
                        offset = pullResult.getNextBeginOffset();
                        break;
                    default:
                        Assert.fail(String.format(Locale.ENGLISH, "unexpected pull offset=%d status: %s", offset, pullResult));
                }
            }
        }
    }

    @Test
    public void test006_LogicalQueueNumChanged() throws Exception {
        String methodName = getCurrentMethodName();
        int logicalQueueNum = brokerNum * QUEUE_NUMBERS;

        List<MessageQueue> publishMessageQueues;
        publishMessageQueues = producer.fetchPublishMessageQueues(topic);
        assertThat(publishMessageQueues).hasSize(logicalQueueNum);
        List<MessageQueue> subscribeMessageQueues;
        subscribeMessageQueues = consumer.fetchSubscribeMessageQueues(topic).stream().sorted().collect(Collectors.toList());
        assertThat(subscribeMessageQueues).hasSize(logicalQueueNum);

        logicalQueueNum++;
        new UpdateTopicLogicalQueueNumCommand().execute(mqAdminExt, clusterName, topic, logicalQueueNum);

        int newAddLogicalQueueIdx = logicalQueueNum - 1;
        MessageQueue newAddLogicalQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, newAddLogicalQueueIdx);
        String newAddLogicalQueueBrokerName;
        {
            TopicRouteData topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
            LogicalQueuesInfo info = topicRouteInfo.getLogicalQueuesInfo();
            assertThat(info).isNotNull();
            List<LogicalQueueRouteData> queueRouteDataList = info.get(newAddLogicalQueueIdx);
            assertThat(queueRouteDataList).isNotNull();
            assertThat(queueRouteDataList).hasSize(1);
            LogicalQueueRouteData queueRouteData = queueRouteDataList.get(0);
            newAddLogicalQueueBrokerName = queueRouteData.getBrokerName();
            assertThat(queueRouteData.getState()).isEqualTo(MessageQueueRouteState.Normal);
            assertThat(queueRouteData.getLogicalQueueDelta()).isEqualTo(0);
            assertThat(queueRouteData.getLogicalQueueIndex()).isEqualTo(newAddLogicalQueueIdx);
        }

        publishMessageQueues = producer.fetchPublishMessageQueues(topic);
        assertThat(publishMessageQueues).hasSize(logicalQueueNum);
        Set<Integer> logicalQueueIds = IntStream.range(0, logicalQueueNum).boxed().collect(Collectors.toSet());
        Map<String, Set<Integer>> queueIds = Maps.newHashMap();
        for (String brokerName : Arrays.asList(broker1Name, broker2Name)) {
            queueIds.put(brokerName, IntStream.range(0, QUEUE_NUMBERS).boxed().collect(Collectors.toSet()));
        }
        queueIds.get(newAddLogicalQueueBrokerName).add(QUEUE_NUMBERS);
        for (MessageQueue messageQueue : publishMessageQueues) {
            assertThat(messageQueue.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
            assertThat(logicalQueueIds.remove(messageQueue.getQueueId())).isTrue();
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-%d-%d", methodName, messageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), messageQueue);
                assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(messageQueue.getBrokerName());
                assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(messageQueue.getQueueId());
                if (i == 0) {
                    SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
                    assertThat(queueIds.get(sendResult2.getOrigBrokerName()).remove(sendResult2.getOrigQueueId())).as("brokerName %s queueId %d", sendResult2.getOrigBrokerName(), sendResult2.getOrigQueueId()).isTrue();
                }
            }
        }
        assertThat(logicalQueueIds).isEmpty();

        subscribeMessageQueues = consumer.fetchSubscribeMessageQueues(topic).stream().sorted().collect(Collectors.toList());
        assertThat(subscribeMessageQueues).hasSize(logicalQueueNum);
        subscribeMessageQueues.sort(Comparator.comparingInt(MessageQueue::getQueueId));
        logicalQueueIds.addAll(IntStream.range(0, logicalQueueNum).boxed().collect(Collectors.toSet()));
        for (MessageQueue messageQueue : subscribeMessageQueues) {
            assertThat(messageQueue.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
            assertThat(logicalQueueIds.remove(messageQueue.getQueueId())).isTrue();
            long offset = mqAdminExt.minOffset(messageQueue);
            assertThat(offset).isEqualTo(0);
            PullResult pullResult = consumer.pull(messageQueue, "*", offset, 10);
            assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
            assertThat(pullResult.getMsgFoundList()).hasSize(MSG_SENT_TIMES);
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                MessageExt msg = pullResult.getMsgFoundList().get(i);
                assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
                assertThat(msg.getQueueId()).isEqualTo(messageQueue.getQueueId());
                assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-%d-%d", methodName, messageQueue.getQueueId(), i));
                assertThat(msg.getQueueOffset()).isEqualTo(offset + i);
            }
            assertThat(maxOffsetUncommitted(messageQueue)).isEqualTo(offset + MSG_SENT_TIMES);
        }
        assertThat(logicalQueueIds).isEmpty();

        // increase TopicConfig write queue first then increase logical queue, expect to reuse
        String broker2Addr = brokerController2.getBrokerAddr();
        TopicConfig topicConfig = mqAdminExt.examineTopicConfig(broker2Addr, topic);
        topicConfig.setWriteQueueNums(topicConfig.getWriteQueueNums() + 1);
        topicConfig.setReadQueueNums(topicConfig.getReadQueueNums() + 1);
        mqAdminExt.createAndUpdateTopicConfig(broker2Addr, topicConfig);
        logicalQueueNum++;
        new UpdateTopicLogicalQueueNumCommand().execute(mqAdminExt, clusterName, topic, logicalQueueNum);
        {
            newAddLogicalQueueIdx = logicalQueueNum -1;
            TopicRouteData topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
            LogicalQueuesInfo info = topicRouteInfo.getLogicalQueuesInfo();
            assertThat(info).isNotNull();
            List<LogicalQueueRouteData> queueRouteDataList = info.get(newAddLogicalQueueIdx);
            assertThat(queueRouteDataList).isNotNull();
            assertThat(queueRouteDataList).hasSize(1);
            LogicalQueueRouteData queueRouteData = queueRouteDataList.get(0);
            assertThat(queueRouteData.getState()).isEqualTo(MessageQueueRouteState.Normal);
            assertThat(queueRouteData.getLogicalQueueDelta()).isEqualTo(0);
            assertThat(queueRouteData.getLogicalQueueIndex()).isEqualTo(newAddLogicalQueueIdx);
            assertThat(queueRouteData.getBrokerName()).isEqualTo(broker2Name);
            assertThat(queueRouteData.getQueueId()).isEqualTo(topicConfig.getWriteQueueNums() -1);
        }

        logicalQueueNum-=2;
        new UpdateTopicLogicalQueueNumCommand().execute(mqAdminExt, clusterName, topic, logicalQueueNum);

        try {
            producer.send(new Message(topic, "aaa".getBytes(StandardCharsets.UTF_8)), newAddLogicalQueue);
            Assert.fail("write to decreased logical queue success, want it failed");
        } catch (MQBrokerException e) {
            assertThat(e.getResponseCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        }
        {
            int offset = 0;
            PullResult pullResult = consumer.pull(newAddLogicalQueue, "*", offset, 10);
            assertThat(pullResult.getPullStatus()).isEqualTo(PullStatus.FOUND);
            assertThat(pullResult.getMsgFoundList()).hasSize(MSG_SENT_TIMES);
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                MessageExt msg = pullResult.getMsgFoundList().get(i);
                assertThat(msg.getBrokerName()).isEqualTo(MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME);
                assertThat(msg.getQueueId()).isEqualTo(newAddLogicalQueue.getQueueId());
                assertThat(new String(msg.getBody(), StandardCharsets.UTF_8)).isEqualTo(String.format(Locale.ENGLISH, "%s-%d-%d", methodName, newAddLogicalQueue.getQueueId(), i));
                assertThat(msg.getQueueOffset()).isEqualTo(offset + i);
            }
        }

        // rotate to remove new add queue's data, and try pull again
        {
            BrokerController brokerController = brokerControllerMap.get(newAddLogicalQueueBrokerName);
            rotateBrokerCommitLog(brokerController);
            deleteCommitLogFiles(brokerController, 1);
        }
        {
            int offset = 0;
            PullResult pullResult = consumer.pull(newAddLogicalQueue, "*", offset, 10);
            assertThat(pullResult.getPullStatus()).isIn(PullStatus.NO_NEW_MSG, PullStatus.NO_MATCHED_MSG);
        }
    }

    @Test
    public void test007_LogicalQueueWritableEvenBrokerDown() throws Exception {
        final String methodName = getCurrentMethodName();

        final int logicalQueueIdx = 1;

        BrokerController brokerController3 = IntegrationTestBase.createAndStartBroker(nsAddr);
        String broker3Name = brokerController3.getBrokerConfig().getBrokerName();
        brokerControllerMap.put(broker3Name, brokerController3);
        await().atMost(30, TimeUnit.SECONDS).until(() -> mqAdminExt.examineBrokerClusterInfo().getBrokerAddrTable().containsKey(broker3Name));
        mqAdminExt.createAndUpdateTopicConfig(brokerController3.getBrokerAddr(), new TopicConfig(topic, 0, 0, PermName.PERM_READ | PermName.PERM_WRITE));

        new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, brokerController3.getBrokerConfig().getBrokerName(), null);

        MessageQueue migrateMessageQueue = new MessageQueue(topic, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME, logicalQueueIdx);
        {
            for (int i = 0; i < MSG_SENT_TIMES; i++) {
                SendResult sendResult = producer.send(new Message(topic, String.format(Locale.ENGLISH, "%s-%d-%d", methodName, migrateMessageQueue.getQueueId(), i).getBytes(StandardCharsets.UTF_8)), migrateMessageQueue);
                assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(migrateMessageQueue.getBrokerName());
                assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(migrateMessageQueue.getQueueId());
                SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
                assertThat(sendResult2.getOrigBrokerName()).isEqualTo(broker3Name);
                assertThat(sendResult2.getOrigQueueId()).isEqualTo(0);
            }
        }
        brokerController3.shutdown();
        brokerControllerMap.remove(broker3Name);

        assertThatThrownBy(() -> {
            SendResult sendResult = producer.send(new Message(topic, "aaa".getBytes(StandardCharsets.UTF_8)), migrateMessageQueue);
            logger.error("send should fail but got {}", sendResult);
        }).isInstanceOf(RemotingException.class).hasMessageMatching("connect to [0-9.:]+ failed");

        assertThatThrownBy(() -> {
            new MigrateTopicLogicalQueueCommand().execute(mqAdminExt, topic, logicalQueueIdx, broker1Name, null);
        }).hasRootCauseInstanceOf(RemotingConnectException.class).hasMessageContaining("migrateTopicLogicalQueuePrepare");

        {
            SendResult sendResult = producer.send(new Message(topic, "aaa".getBytes(StandardCharsets.UTF_8)), migrateMessageQueue);
            assertThat(sendResult.getMessageQueue().getBrokerName()).isEqualTo(migrateMessageQueue.getBrokerName());
            assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(migrateMessageQueue.getQueueId());
            assertThat(sendResult.getQueueOffset()).isEqualTo(-1);
            SendResultForLogicalQueue sendResult2 = (SendResultForLogicalQueue) sendResult;
            assertThat(sendResult2.getOrigBrokerName()).isEqualTo(broker1Name);
            assertThat(sendResult2.getOrigQueueId()).isIn(
                /* CommitLog not rotated, will not reuse */QUEUE_NUMBERS,
                /* CommitLog rotated in other test cases, will reuse */logicalQueueIdx
            );
        }
    }

    private static String getBrokerCommitLogFileName(BrokerController brokerController) throws IllegalAccessException {
        DefaultMessageStore defaultMessageStore = (DefaultMessageStore) brokerController.getMessageStore();
        MappedFileQueue mfq = (MappedFileQueue) FieldUtils.readDeclaredField(defaultMessageStore.getCommitLog(), "mappedFileQueue", true);
        return mfq.getLastMappedFile().getFileName();
    }

    private static void deleteCommitLogFiles(BrokerController brokerController,
        int keepNum) throws IllegalAccessException {
        CommitLog commitLog = ((DefaultMessageStore) brokerController.getMessageStore()).getCommitLog();
        commitLog.flush();
        MappedFileQueue mfq = (MappedFileQueue) FieldUtils.readDeclaredField(commitLog, "mappedFileQueue", true);
        AtomicInteger count = new AtomicInteger();
        waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            count.getAndAdd(commitLog.deleteExpiredFile(0, 0, 5000, true, 1));
            return mfq.getMappedFiles().size() <= keepNum;
        });
        brokerController.getTopicConfigManager().getLogicalQueueCleanHook().execute((DefaultMessageStore) brokerController.getMessageStore(), count.get());
        logger.info("deleteCommitLogFiles {} count {}", brokerController.getBrokerConfig().getBrokerName(), count.get());
    }

    private static void rotateBrokerCommitLog(BrokerController brokerController) throws IllegalAccessException {
        CommitLog commitLog = ((DefaultMessageStore) brokerController.getMessageStore()).getCommitLog();
        commitLog.flush();
        String brokerName = brokerController.getBrokerConfig().getBrokerName();
        String fileName1 = getBrokerCommitLogFileName(brokerController);
        logger.info("rotateBrokerCommitLog {} first {}", brokerName, fileName1);
        int msgSize = 4 * 1024;
        byte[] data = RandomStringUtils.randomAscii(msgSize).getBytes(StandardCharsets.UTF_8);
        Message msg = new Message(placeholderTopic, data);
        MessageQueue mq = new MessageQueue(placeholderTopic, brokerName, 0);
        waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            for (int i = 0; i < 128; i++) {
                producer.send(msg, mq);
            }
            commitLog.flush();
            String fileName2 = getBrokerCommitLogFileName(brokerController);
            if (!fileName1.equals(fileName2)) {
                logger.info("rotateBrokerCommitLog {} 4K msg last {}", brokerName, fileName2);
                return true;
            }
            return false;
        });
    }

    private long maxOffsetUncommitted(MessageQueue mq) throws IllegalAccessException, MQClientException {
        DefaultMQAdminExtImpl defaultMQAdminExtImpl = (DefaultMQAdminExtImpl) FieldUtils.readDeclaredField(mqAdminExt, "defaultMQAdminExtImpl", true);
        return defaultMQAdminExtImpl.maxOffset(mq, false);
    }
}
