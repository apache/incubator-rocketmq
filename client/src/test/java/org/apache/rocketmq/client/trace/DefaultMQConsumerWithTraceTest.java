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

package org.apache.rocketmq.client.trace;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;

import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DefaultMQPushConsumerImpl.class)
public class DefaultMQConsumerWithTraceTest {
    private String consumerGroup;
    private String consumerGroupNormal;
    private String producerGroupTraceTemp = MixAll.RMQ_SYS_TRACE_TOPIC + System.currentTimeMillis();

    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private PullAPIWrapper pullAPIWrapper;
    private RebalancePushImpl rebalancePushImpl;
    private DefaultMQPushConsumer pushConsumer;
    private DefaultMQPushConsumer normalPushConsumer;
    private DefaultMQPushConsumer customTraceTopicpushConsumer;

    private AsyncTraceDispatcher asyncTraceDispatcher;
    private MQClientInstance mQClientTraceFactory;
    @Mock
    private MQClientAPIImpl mQClientTraceAPIImpl;
    private DefaultMQProducer traceProducer;
    private String customerTraceTopic = "rmq_trace_topic_12345";

    @Before
    public void init() throws Exception {
        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup, true, "");
        consumerGroupNormal = "FooBarGroup" + System.currentTimeMillis();
        normalPushConsumer = new DefaultMQPushConsumer(consumerGroupNormal, false, "");
        customTraceTopicpushConsumer = new DefaultMQPushConsumer(consumerGroup, true, customerTraceTopic);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.setPullInterval(60 * 1000);

        asyncTraceDispatcher = (AsyncTraceDispatcher) pushConsumer.getTraceDispatcher();
        traceProducer = asyncTraceDispatcher.getTraceProducer();

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                return null;
            }
        });

        PowerMockito.suppress(PowerMockito.method(DefaultMQPushConsumerImpl.class, "updateTopicSubscribeInfoWhenSubscriptionChanged"));
        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("rebalanceImpl");
        field.setAccessible(true);
        field.set(pushConsumerImpl, rebalancePushImpl);
        pushConsumer.subscribe(topic, "*");

        pushConsumer.start();

        mQClientFactory = spy(pushConsumerImpl.getmQClientFactory());
        mQClientTraceFactory = spy(pushConsumerImpl.getmQClientFactory());

        field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pushConsumerImpl, mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        Field fieldTrace = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        fieldTrace.setAccessible(true);
        fieldTrace.set(traceProducer.getDefaultMQProducerImpl(), mQClientTraceFactory);

        fieldTrace = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        fieldTrace.setAccessible(true);
        fieldTrace.set(mQClientTraceFactory, mQClientTraceAPIImpl);

        pullAPIWrapper = spy(new PullAPIWrapper(mQClientFactory, consumerGroup, false));
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("pullAPIWrapper");
        field.setAccessible(true);
        field.set(pushConsumerImpl, pullAPIWrapper);

        pushConsumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().setmQClientFactory(mQClientFactory);
        mQClientFactory.registerConsumer(consumerGroup, pushConsumerImpl);

        when(mQClientFactory.getMQClientAPIImpl().pullMessage(anyString(), any(PullMessageRequestHeader.class),
            anyLong(), any(CommunicationMode.class), nullable(PullCallback.class)))
            .thenAnswer(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock mock) throws Throwable {
                    PullMessageRequestHeader requestHeader = mock.getArgument(1);
                    MessageClientExt messageClientExt = new MessageClientExt();
                    messageClientExt.setTopic(topic);
                    messageClientExt.setQueueId(0);
                    messageClientExt.setMsgId("123");
                    messageClientExt.setBody(new byte[] {'a'});
                    messageClientExt.setOffsetMsgId("234");
                    messageClientExt.setBornHost(new InetSocketAddress(8080));
                    messageClientExt.setStoreHost(new InetSocketAddress(8080));
                    PullResult pullResult = createPullResult(requestHeader, PullStatus.FOUND, Collections.<MessageExt>singletonList(messageClientExt));
                    ((PullCallback) mock.getArgument(4)).onSuccess(pullResult);
                    return pullResult;
                }
            });

        doReturn(new FindBrokerResult("127.0.0.1:10911", false)).when(mQClientFactory).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());
        Set<MessageQueue> messageQueueSet = new HashSet<MessageQueue>();
        messageQueueSet.add(createPullRequest().getMessageQueue());
        pushConsumer.getDefaultMQPushConsumerImpl().updateTopicSubscribeInfo(topic, messageQueueSet);
    }

    @After
    public void terminate() {
        pushConsumer.shutdown();
    }

    @Test
    public void testPullMessage_WithTrace_Success() throws InterruptedException, RemotingException, MQBrokerException, MQClientException {
        traceProducer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTraceTemp, traceProducer.getDefaultMQProducerImpl());

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                messageExts[0] = msgs.get(0);
                countDownLatch.countDown();
                return null;
            }
        }));

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
        assertThat(messageExts[0].getTopic()).isEqualTo(topic);
        assertThat(messageExts[0].getBody()).isEqualTo(new byte[] {'a'});
    }

    private PullRequest createPullRequest() {
        PullRequest pullRequest = new PullRequest();
        pullRequest.setConsumerGroup(consumerGroup);
        pullRequest.setNextOffset(1024);

        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName(brokerName);
        messageQueue.setQueueId(0);
        messageQueue.setTopic(topic);
        pullRequest.setMessageQueue(messageQueue);
        ProcessQueue processQueue = new ProcessQueue();
        processQueue.setLocked(true);
        processQueue.setLastLockTimestamp(System.currentTimeMillis());
        pullRequest.setProcessQueue(processQueue);

        return pullRequest;
    }

    private PullResultExt createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
        List<MessageExt> messageExtList) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (MessageExt messageExt : messageExtList) {
            outputStream.write(MessageDecoder.encode(messageExt, false));
        }
        return new PullResultExt(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList, 0, outputStream.toByteArray());
    }

    public static TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSynFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

    public static TopicRouteData createTraceTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("broker-trace");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10912");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-trace");
        queueData.setPerm(6);
        queueData.setReadQueueNums(1);
        queueData.setWriteQueueNums(1);
        queueData.setTopicSynFlag(1);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }
}
