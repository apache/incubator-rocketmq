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

import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumeMessageConcurrentlyServiceTest {
    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private PullAPIWrapper pullAPIWrapper;
    private RebalancePushImpl rebalancePushImpl;
    private DefaultMQPushConsumer pushConsumer;

    @Before
    public void init() throws Exception {
        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.setPullInterval(60 * 1000);

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("rebalanceImpl");
        field.setAccessible(true);
        field.set(pushConsumerImpl, rebalancePushImpl);
        pushConsumer.subscribe(topic, "*");
        pushConsumer.start();

        mQClientFactory = spy(pushConsumerImpl.getmQClientFactory());
        field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pushConsumerImpl, mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

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

        doReturn(new FindBrokerResult("127.0.0.1:10912", false)).when(mQClientFactory).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());
        Set<MessageQueue> messageQueueSet = new HashSet<MessageQueue>();
        messageQueueSet.add(createPullRequest().getMessageQueue());
        pushConsumer.getDefaultMQPushConsumerImpl().updateTopicSubscribeInfo(topic, messageQueueSet);
    }

    @Ignore
    @Test
    public void testPullMessage_ConsumeSuccess() throws InterruptedException, RemotingException, MQBrokerException, NoSuchFieldException,Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];

        ConsumeMessageConcurrentlyService  normalServie = new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                messageExts[0] = msgs.get(0);
                countDownLatch.countDown();
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(normalServie);

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await();

        Thread.sleep(1000);

        org.apache.rocketmq.common.protocol.body.ConsumeStatus stats = normalServie.getConsumerStatsManager().consumeStatus(pushConsumer.getDefaultMQPushConsumerImpl().groupName(),topic);

        ConsumerStatsManager mgr  =   normalServie.getConsumerStatsManager();

        Field statItmeSetField = mgr.getClass().getDeclaredField("topicAndGroupConsumeOKTPS");
        statItmeSetField.setAccessible(true);

        StatsItemSet itemSet = (StatsItemSet)statItmeSetField.get(mgr);
        StatsItem item = itemSet.getAndCreateStatsItem(topic + "@" + pushConsumer.getDefaultMQPushConsumerImpl().groupName());

        assertThat(item.getValue().get()).isGreaterThan(0L);
        assertThat(messageExts[0].getTopic()).isEqualTo(topic);
        assertThat(messageExts[0].getBody()).isEqualTo(new byte[] {'a'});
    }

    @After
    public void terminate() {
        pushConsumer.shutdown();
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
}
