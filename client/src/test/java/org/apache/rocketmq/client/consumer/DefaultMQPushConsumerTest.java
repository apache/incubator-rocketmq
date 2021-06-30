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
package org.apache.rocketmq.client.consumer;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQPushConsumerTest {
    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private MQClientInstance mQClientFactory;

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private RebalanceImpl rebalanceImpl;
    private RebalancePushImpl rebalancePushImpl;
    private DefaultMQPushConsumer pushConsumer;

    @Before
    public void init() throws Exception {
        ConcurrentMap<String, MQClientInstance> factoryTable = (ConcurrentMap<String, MQClientInstance>) FieldUtils.readDeclaredField(MQClientManager.getInstance(), "factoryTable", true);
        factoryTable.forEach((s, instance) -> instance.shutdown());
        factoryTable.clear();

        when(mQClientAPIImpl.pullMessage(anyString(), any(PullMessageRequestHeader.class),
            anyLong(), any(CommunicationMode.class), nullable(PullCallback.class)))
            .thenAnswer(new Answer<PullResult>() {
                @Override
                public PullResult answer(InvocationOnMock mock) throws Throwable {
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


        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.setPullInterval(60 * 1000);

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                return null;
            }
        });

        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));

        // suppress updateTopicRouteInfoFromNameServer
        pushConsumer.changeInstanceNameToPID();
        mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(pushConsumer, (RPCHook) FieldUtils.readDeclaredField(pushConsumerImpl, "rpcHook", true));
        FieldUtils.writeDeclaredField(mQClientFactory, "mQClientAPIImpl", mQClientAPIImpl, true);
        mQClientFactory = spy(mQClientFactory);
        factoryTable.put(pushConsumer.buildMQClientId(), mQClientFactory);
        doReturn(false).when(mQClientFactory).updateTopicRouteInfoFromNameServer(anyString());

        doReturn(new FindBrokerResult("127.0.0.1:10911", false)).when(mQClientFactory).findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean());

        rebalanceImpl = spy(pushConsumerImpl.getRebalanceImpl());
        doReturn(123L).when(rebalanceImpl).computePullFromWhereWithException(any(MessageQueue.class));
        FieldUtils.writeDeclaredField(pushConsumerImpl, "rebalanceImpl", rebalanceImpl, true);

        Set<MessageQueue> messageQueueSet = new HashSet<MessageQueue>();
        messageQueueSet.add(createPullRequest().getMessageQueue());
        pushConsumerImpl.updateTopicSubscribeInfo(topic, messageQueueSet);

        pushConsumer.subscribe(topic, "*");
        pushConsumer.start();
    }

    @After
    public void terminate() {
        pushConsumer.shutdown();
    }

    @Test
    public void testStart_OffsetShouldNotNUllAfterStart() {
        Assert.assertNotNull(pushConsumer.getOffsetStore());
    }

    @Test
    public void testPullMessage_Success() throws InterruptedException, RemotingException, MQBrokerException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<MessageExt> messageAtomic = new AtomicReference<>();
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                messageAtomic.set(msgs.get(0));
                countDownLatch.countDown();
                return null;
            }
        }));

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        countDownLatch.await(10, TimeUnit.SECONDS);
        MessageExt msg = messageAtomic.get();
        assertThat(msg).isNotNull();
        assertThat(msg.getTopic()).isEqualTo(topic);
        assertThat(msg.getBody()).isEqualTo(new byte[] {'a'});
    }

    @Test
    public void testPullMessage_SuccessWithOrderlyService() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<MessageExt> messageAtomic = new AtomicReference<>();

        MessageListenerOrderly listenerOrderly = new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                messageAtomic.set(msgs.get(0));
                countDownLatch.countDown();
                return null;
            }
        };
        pushConsumer.registerMessageListener(listenerOrderly);
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), listenerOrderly));
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeOrderly(true);
        pushConsumer.getDefaultMQPushConsumerImpl().doRebalance();
        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestLater(createPullRequest(), 100);

        countDownLatch.await(10, TimeUnit.SECONDS);
        MessageExt msg = messageAtomic.get();
        assertThat(msg).isNotNull();
        assertThat(msg.getTopic()).isEqualTo(topic);
        assertThat(msg.getBody()).isEqualTo(new byte[] {'a'});
    }

    @Test
    public void testCheckConfig() {
        DefaultMQPushConsumer pushConsumer = createPushConsumer();

        pushConsumer.setPullThresholdForQueue(65535 + 1);
        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdForQueue Out of range [1, 65535]");
        }

        pushConsumer = createPushConsumer();
        pushConsumer.setPullThresholdForTopic(65535 * 100 + 1);

        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdForTopic Out of range [1, 6553500]");
        }

        pushConsumer = createPushConsumer();
        pushConsumer.setPullThresholdSizeForQueue(1024 + 1);
        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdSizeForQueue Out of range [1, 1024]");
        }

        pushConsumer = createPushConsumer();
        pushConsumer.setPullThresholdSizeForTopic(1024 * 100 + 1);
        try {
            pushConsumer.start();
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("pullThresholdSizeForTopic Out of range [1, 102400]");
        }
    }

    @Test
    public void testGracefulShutdown() throws InterruptedException, RemotingException, MQBrokerException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        pushConsumer.setAwaitTerminationMillisWhenShutdown(2000);
        final AtomicBoolean messageConsumedFlag = new AtomicBoolean(false);
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(), new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                countDownLatch.countDown();
                try {
                    Thread.sleep(1000);
                    messageConsumedFlag.set(true);
                } catch (InterruptedException e) {
                }

                return null;
            }
        }));

        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();

        pushConsumer.shutdown();
        assertThat(messageConsumedFlag.get()).isTrue();
    }

    @Test
    public void testGetServiceState() {
        DefaultMQPushConsumer pushConsumer = createPushConsumer();
        ServiceState serviceState = pushConsumer.getServiceState();
        Assert.assertEquals(ServiceState.CREATE_JUST, serviceState);
    }

    private DefaultMQPushConsumer createPushConsumer() {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                return null;
            }
        });
        return pushConsumer;
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

    @Test
    public void testPullMessage_ExceptionOccursWhenComputePullFromWhere() throws MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MessageExt[] messageExts = new MessageExt[1];
        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeMessageService(
                new ConsumeMessageConcurrentlyService(pushConsumer.getDefaultMQPushConsumerImpl(),
                        (msgs, context) -> {
                            messageExts[0] = msgs.get(0);
                            return null;
                        }));

        pushConsumer.getDefaultMQPushConsumerImpl().setConsumeOrderly(true);
        PullMessageService pullMessageService = mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(createPullRequest());
        assertThat(messageExts[0]).isNull();
    }
}
