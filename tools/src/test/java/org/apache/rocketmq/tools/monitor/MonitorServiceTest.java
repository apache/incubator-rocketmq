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
package org.apache.rocketmq.tools.monitor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.rocketmq.common.protocol.heartbeat.ConsumeType.CONSUME_ACTIVELY;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MonitorServiceTest {
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;
    private static MonitorConfig monitorConfig;
    private static MonitorListener monitorListener;
    private static DefaultMQPullConsumer defaultMQPullConsumer;
    private static DefaultMQPushConsumer defaultMQPushConsumer;
    private static MonitorService monitorService;

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, RemotingException, MQClientException, InterruptedException, MQBrokerException {
        monitorConfig = new MonitorConfig();
        monitorListener = new DefaultMonitorListener();
        defaultMQPullConsumer = mock(DefaultMQPullConsumer.class);
        defaultMQPushConsumer = mock(DefaultMQPushConsumer.class);
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);
        monitorService = new MonitorService(monitorConfig, monitorListener, null);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        field = MonitorService.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(monitorService, defaultMQAdminExt);
        field = MonitorService.class.getDeclaredField("defaultMQPullConsumer");
        field.setAccessible(true);
        field.set(monitorService, defaultMQPullConsumer);
        field = MonitorService.class.getDeclaredField("defaultMQPushConsumer");
        field.setAccessible(true);
        field.set(monitorService, defaultMQPushConsumer);

        TopicList topicList = new TopicList();
        Set<String> topicSet = new HashSet<>();
        topicSet.add("topic_one");
        topicSet.add("topic_two");
        topicList.setTopicList(topicSet);
        when(mQClientAPIImpl.getTopicListFromNameServer(anyLong())).thenReturn(topicList);

        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(1234l, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("default-cluster");
        brokerData.setBrokerName("default-broker");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setConsumeTps(1234);
        MessageQueue messageQueue = new MessageQueue();
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        HashMap<MessageQueue, OffsetWrapper> stats = new HashMap<>();
        stats.put(messageQueue, offsetWrapper);
        consumeStats.setOffsetTable(stats);
        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), anyString(), anyLong())).thenReturn(consumeStats);

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerConnection.setMessageModel(MessageModel.CLUSTERING);
        HashSet<Connection> connections = new HashSet<>();
        Connection connection = new Connection();
        connection.setClientId("client_id");
        connection.setClientAddr("127.0.0.1:109111");
        connection.setLanguage(LanguageCode.JAVA);
        connection.setVersion(MQVersion.Version.V4_0_0_SNAPSHOT.ordinal());
        connections.add(connection);
        consumerConnection.setConnectionSet(connections);
        consumerConnection.setSubscriptionTable(new ConcurrentHashMap<String, SubscriptionData>());
        consumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        consumerRunningInfo.setSubscriptionSet(new TreeSet<SubscriptionData>());
        Properties properties = new Properties();
        properties.put(ConsumerRunningInfo.PROP_CONSUME_TYPE, CONSUME_ACTIVELY);
        properties.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, System.currentTimeMillis());
        consumerRunningInfo.setProperties(properties);
        when(mQClientAPIImpl.getConsumerRunningInfo(anyString(), anyString(), anyString(), anyBoolean(), anyLong())).thenReturn(consumerRunningInfo);
    }

    @AfterClass
    public static void terminate() {
    }

    @Test
    public void testDoMonitorWork() throws RemotingException, MQClientException, InterruptedException {
        monitorService.doMonitorWork();
    }

    @Test
    public void testReportConsumerRunningInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        monitorService.reportConsumerRunningInfo("test_group");
    }
}