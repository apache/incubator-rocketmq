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
package org.apache.rocketmq.client.impl.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class MQClientInstanceTest {
    private MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private String topic = "FooBar";
    private String group = "FooBarGroup";

    @Test
    public void testTopicRouteData2TopicPublishInfo() {
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

        TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);

        assertThat(topicPublishInfo.isHaveTopicRouterInfo()).isFalse();
        assertThat(topicPublishInfo.getMessageQueueList().size()).isEqualTo(4);
    }

    @Test
    public void testRegisterProducer() {
        boolean flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterProducer(group);
        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterConsumer() throws RemotingException, InterruptedException, MQBrokerException {
        boolean flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterAdminExt() {
        boolean flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterAdminExt(group);
        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();
    }
}