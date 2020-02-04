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

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerWithTraceTest {

    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    private AsyncTraceDispatcher asyncTraceDispatcher;

    private DefaultMQProducer producer;
    private DefaultMQProducer customTraceTopicproducer;
    private DefaultMQProducer traceProducer;
    private DefaultMQProducer normalProducer;

    private Message message;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";
    private String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
    private String producerGroupTraceTemp = MixAll.RMQ_SYS_TRACE_TOPIC + System.currentTimeMillis();
    private String customerTraceTopic = "rmq_trace_topic_12345";

    @Before
    public void init() throws Exception {

        customTraceTopicproducer = new DefaultMQProducer(producerGroupTemp, false, customerTraceTopic);
        normalProducer = new DefaultMQProducer(producerGroupTemp, false, "");
        producer = new DefaultMQProducer(producerGroupTemp, true, "");
        producer.setNamesrvAddr("127.0.0.1:9876");
        normalProducer.setNamesrvAddr("127.0.0.1:9877");
        customTraceTopicproducer.setNamesrvAddr("127.0.0.1:9878");
        message = new Message(topic, new byte[] {'a', 'b', 'c'});
        asyncTraceDispatcher = (AsyncTraceDispatcher) producer.getTraceDispatcher();
        asyncTraceDispatcher.setTraceTopicName(customerTraceTopic);
        asyncTraceDispatcher.getHostProducer();
        asyncTraceDispatcher.getHostConsumer();
        traceProducer = asyncTraceDispatcher.getTraceProducer();

        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        Field fieldTrace = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        fieldTrace.setAccessible(true);
        fieldTrace.set(traceProducer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));

    }

    @Test
    public void testSendMessageSync_WithTrace_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        traceProducer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTraceTemp, traceProducer.getDefaultMQProducerImpl());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            producer.send(message);
        } catch (MQClientException e) {
        }
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);

    }

    @Test
    public void testSendMessageSync_WithTrace_NoBrokerSet_Exception() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            producer.send(message);
        } catch (MQClientException e) {
        }
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);

    }

    @After
    public void terminate() {
        producer.shutdown();
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
