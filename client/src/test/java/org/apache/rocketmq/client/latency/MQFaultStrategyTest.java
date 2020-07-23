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

package org.apache.rocketmq.client.latency;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class MQFaultStrategyTest {
    private MQFaultStrategy mqFaultStrategy;
    @Spy
    private final TopicPublishInfo tpInfo = createTopicPublicshInfo();
    @Spy
    private final LatencyFaultToleranceImpl latencyFaultTolerance =  new LatencyFaultToleranceImpl();
    @Mock
    private ThreadLocalIndex sendWhichQueue;
    private static String brokerNameA = "BrokerA";
    private static String brokerNameB = "BrokerB";
    private static String brokerNameC = "BrokerC";
    private static String topic = "TEST_TOPIC";

    @Before
    public void init() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        this.mqFaultStrategy = new MQFaultStrategy();
        Field field = MQFaultStrategy.class.getDeclaredField("latencyFaultTolerance");
        field.setAccessible(true);
        field.set(mqFaultStrategy, latencyFaultTolerance);
        mqFaultStrategy.setSendLatencyFaultEnable(true);
        this.tpInfo.setSendWhichQueue(sendWhichQueue);
    }

    @Test
    public void testSelectOneMessageQueue_NullLastBroker() throws Exception {
        doReturn(0).when(sendWhichQueue).getAndIncrement();
        MessageQueue mqChosen = mqFaultStrategy.selectOneMessageQueue(tpInfo, null);
        String brokerChosen = mqChosen.getBrokerName();
        assertThat(brokerChosen).isEqualTo(brokerNameA);
    }

    @Test
    public void testSelectOneMessageQueue_FaultBrokerA() throws Exception {
        doReturn(0).when(sendWhichQueue).getAndIncrement();
        mqFaultStrategy.updateFaultItem(brokerNameA, 3000, true);
        MessageQueue mqChosen = mqFaultStrategy.selectOneMessageQueue(tpInfo, brokerNameA);
        String brokerChosen = mqChosen.getBrokerName();
        assertThat(brokerChosen).isEqualTo(brokerNameB);
    }

    @Test
    public void testSelectOneMessageQueue_FaultBrokerAB() throws Exception {
        doReturn(0).when(sendWhichQueue).getAndIncrement();
        mqFaultStrategy.updateFaultItem(brokerNameA, 3000, true);
        mqFaultStrategy.updateFaultItem(brokerNameB, 3000, true);
        MessageQueue mqChosen = mqFaultStrategy.selectOneMessageQueue(tpInfo, brokerNameB);
        String brokerChosen = mqChosen.getBrokerName();
        assertThat(brokerChosen).isEqualTo(brokerNameC);
    }

    @Test
    public void testSelectOneMessageQueue_FaultBrokerABC() throws Exception {       
        doReturn(0).when(sendWhichQueue).getAndIncrement();
        mqFaultStrategy.updateFaultItem(brokerNameA, 3000, true);
        mqFaultStrategy.updateFaultItem(brokerNameB, 3000, true);
        mqFaultStrategy.updateFaultItem(brokerNameC, 3000, true);
        MessageQueue mqChosen = mqFaultStrategy.selectOneMessageQueue(tpInfo, brokerNameC);
        String brokerChosen = mqChosen.getBrokerName();
        assertThat(brokerChosen).isNotEqualTo(null);
    }

    @Test
    public void testComputeNotAvailableDuration_MinusInput() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(0L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, -1, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition0() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(0L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 20, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition1() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(0L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 80, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition2() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(0L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 120, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition3() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(30000L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 600, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition4() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(60000L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 1200, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition5() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(120000L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 2300, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition6() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(180000L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 3400, false);
    }

    @Test
    public void testComputeNotAvailableDuration_Condition7() throws Exception {       
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                long duration = mock.getArgument(2);
                assertThat(duration).isEqualTo(600000L);
                return null;
            }
        }).when(latencyFaultTolerance).updateFaultItem(anyString(), anyLong(), anyLong());
        mqFaultStrategy.updateFaultItem(brokerNameA, 16000, false);
    }

    private TopicPublishInfo createTopicPublicshInfo() {
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        List<MessageQueue> messageQueues = new ArrayList<MessageQueue>();
        MessageQueue messagequeueA0 = new MessageQueue(topic, brokerNameA, 0);
        MessageQueue messagequeueA1 = new MessageQueue(topic, brokerNameA, 1);
        MessageQueue messagequeueA2 = new MessageQueue(topic, brokerNameA, 2);
        MessageQueue messagequeueA3 = new MessageQueue(topic, brokerNameA, 3);
        MessageQueue messagequeueB0 = new MessageQueue(topic, brokerNameB, 0);
        MessageQueue messagequeueB1 = new MessageQueue(topic, brokerNameB, 1);
        MessageQueue messagequeueB2 = new MessageQueue(topic, brokerNameB, 2);
        MessageQueue messagequeueB3 = new MessageQueue(topic, brokerNameB, 3);
        MessageQueue messagequeueC0 = new MessageQueue(topic, brokerNameC, 0);
        MessageQueue messagequeueC1 = new MessageQueue(topic, brokerNameC, 1);
        MessageQueue messagequeueC2 = new MessageQueue(topic, brokerNameC, 2);
        MessageQueue messagequeueC3 = new MessageQueue(topic, brokerNameC, 3);
        messageQueues.add(messagequeueA0);
        messageQueues.add(messagequeueA1);
        messageQueues.add(messagequeueA2);
        messageQueues.add(messagequeueA3);
        messageQueues.add(messagequeueB0);
        messageQueues.add(messagequeueB1);
        messageQueues.add(messagequeueB2);
        messageQueues.add(messagequeueB3);
        messageQueues.add(messagequeueC0);
        messageQueues.add(messagequeueC1);
        messageQueues.add(messagequeueC2);
        messageQueues.add(messagequeueC3);
        topicPublishInfo.setMessageQueueList(messageQueues);
        return topicPublishInfo;
    }
}
