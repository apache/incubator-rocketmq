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
package org.apache.rocketmq.tools.command;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommandUtilTest {
    private DefaultMQAdminExt defaultMQAdminExt;
    private DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private MQClientAPIImpl mQClientAPIImpl;

    @Before
    public void setup() throws MQClientException, NoSuchFieldException, IllegalAccessException, InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        defaultMQAdminExt = mock(DefaultMQAdminExt.class);
        MQClientAPIImpl mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 3000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);

        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(1234l, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("default-broker");
        brokerData.setCluster("default-cluster");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerAddrTable.put("default-broker", brokerData);
        brokerAddrTable.put("broker-test", new BrokerData());
        Set<String> brokerSet = new HashSet<>();
        brokerSet.add("default-broker");
        brokerSet.add("default-broker-one");
        clusterAddrTable.put("default-cluster", brokerSet);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        when(mQClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(clusterInfo);
        when(mQClientAPIImpl.cleanExpiredConsumeQueue(anyString(), anyLong())).thenReturn(true);
    }

    @After
    public void shutdown() throws Exception {
    }

    @Test
    public void testFetchMasterAndSlaveDistinguish() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        Map<String, List<String>> result = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExtImpl, "default-cluster");
        assertThat(result.get(null).get(0)).isEqualTo("127.0.0.1:10911");
    }

    @Test
    public void testFetchMasterAddrByClusterName() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        Set<String> result = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExtImpl, "default-cluster");
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testFetchBrokerNameByClusterName() throws Exception {
        Set<String> result = CommandUtil.fetchBrokerNameByClusterName(defaultMQAdminExtImpl, "default-cluster");
        assertThat(result.contains("default-broker")).isTrue();
        assertThat(result.contains("default-broker-one")).isTrue();
        assertThat(result.size()).isEqualTo(2);
    }
}