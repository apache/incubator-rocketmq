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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class RouteInfoManagerTest {

    private static RouteInfoManager routeInfoManager;

    @Before
    public void setup() {
        routeInfoManager = new RouteInfoManager();
        testRegisterBroker();
    }

    @After
    public void terminate() {
        routeInfoManager.printAllPeriodically();
        routeInfoManager.unregisterBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234);
    }

    @Test
    public void testGetAllClusterInfo() {
        byte[] clusterInfo = routeInfoManager.getAllClusterInfo();
        assertThat(clusterInfo).isNotNull();
    }

    @Test
    public void testGetAllTopicList() {
        byte[] topicInfo = routeInfoManager.getAllTopicList();
        Assert.assertTrue(topicInfo != null);
        assertThat(topicInfo).isNotNull();
    }

    @Test
    public void testRegisterBroker() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setWriteQueueNums(8);
        topicConfig.setTopicName("unit-test");
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setOrder(false);
        topicConfigConcurrentHashMap.put("unit-test", topicConfig);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
        Channel channel = mock(Channel.class);
        RegisterBrokerResult registerBrokerResult = routeInfoManager.registerBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234, "127.0.0.1:1001",
                topicConfigSerializeWrapper, new ArrayList<String>(), channel);
        assertThat(registerBrokerResult).isNotNull();
    }

    @Test
    public void testWipeWritePermOfBrokerByLock() throws Exception {
        List<QueueData> qdList = new ArrayList<>();
        QueueData qd = new QueueData();
        qd.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        qd.setBrokerName("broker-a");
        qdList.add(qd);
        HashMap<String, List<QueueData>> topicQueueTable = new HashMap<>();
        topicQueueTable.put("topic-a", qdList);

        Field filed = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        filed.setAccessible(true);
        filed.set(routeInfoManager, topicQueueTable);

        int addTopicCnt = routeInfoManager.wipeWritePermOfBrokerByLock("broker-a");
        assertThat(addTopicCnt).isEqualTo(1);
        assertThat(qd.getPerm()).isEqualTo(PermName.PERM_READ);

    }

    @Test
    public void testPickupTopicRouteData() {
        TopicRouteData result = routeInfoManager.pickupTopicRouteData("unit_test");
        assertThat(result).isNull();
    }

    @Test
    public void testGetSystemTopicList() {
        byte[] topicList = routeInfoManager.getSystemTopicList();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetTopicsByCluster() {
        byte[] topicList = routeInfoManager.getTopicsByCluster("default-cluster");
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetUnitTopics() {
        byte[] topicList = routeInfoManager.getUnitTopics();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetHasUnitSubTopicList() {
        byte[] topicList = routeInfoManager.getHasUnitSubTopicList();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetHasUnitSubUnUnitTopicList() {
        byte[] topicList = routeInfoManager.getHasUnitSubUnUnitTopicList();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testAddWritePermOfBrokerByLock() throws Exception {
        List<QueueData> qdList = new ArrayList<>();
        QueueData qd = new QueueData();
        qd.setPerm(PermName.PERM_READ);
        qd.setBrokerName("broker-a");
        qdList.add(qd);
        HashMap<String, List<QueueData>> topicQueueTable = new HashMap<>();
        topicQueueTable.put("topic-a", qdList);

        Field filed = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        filed.setAccessible(true);
        filed.set(routeInfoManager, topicQueueTable);

        int addTopicCnt = routeInfoManager.addWritePermOfBrokerByLock("broker-a");
        assertThat(addTopicCnt).isEqualTo(1);
        assertThat(qd.getPerm()).isEqualTo(PermName.PERM_READ | PermName.PERM_WRITE);

    }
}