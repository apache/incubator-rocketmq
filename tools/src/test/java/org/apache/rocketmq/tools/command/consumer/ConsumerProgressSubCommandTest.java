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
package org.apache.rocketmq.tools.command.consumer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumerProgressSubCommandTest {
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

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
    }

    @AfterClass
    public static void terminate() {
        defaultMQAdminExt.shutdown();
    }

    @Ignore
    @Test
    public void testExecute() throws SubCommandException {
        ConsumerProgressSubCommand cmd = new ConsumerProgressSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g default-group"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }
}
