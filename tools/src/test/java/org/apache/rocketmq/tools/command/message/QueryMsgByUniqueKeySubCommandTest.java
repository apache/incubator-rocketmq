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
package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.*;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryMsgByUniqueKeySubCommandTest {

    private static QueryMsgByUniqueKeySubCommand cmd = new QueryMsgByUniqueKeySubCommand();

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());

    private static MQClientAPIImpl mQClientAPIImpl;
    private static MQAdminImpl mQAdminImpl;

    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingException, MQClientException, MQBrokerException {

        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        mQAdminImpl = mock(MQAdminImpl.class);

        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);

        field = MQClientInstance.class.getDeclaredField("mQAdminImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQAdminImpl);


        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setConsumeResult(CMResult.CR_SUCCESS);
        result.setRemark("customRemark_122333444");
        when(mQClientAPIImpl.consumeMessageDirectly(anyString(), anyString(), anyString(), anyString(), anyLong())).thenReturn(result);

        MessageExt retMsgExt = new MessageExt();
        retMsgExt.setMsgId("0A3A54F7BF7D18B4AAC28A3FA2CF0000");
        retMsgExt.setBody("this is message ext body".getBytes());
        retMsgExt.setTopic("testTopic");
        retMsgExt.setTags("testTags");
        retMsgExt.setStoreHost(new InetSocketAddress("127.0.0.1", 8899));
        retMsgExt.setBornHost(new InetSocketAddress("127.0.0.1", 7788));
        retMsgExt.setQueueId(1);
        retMsgExt.setQueueOffset(12L);
        retMsgExt.setCommitLogOffset(123);
        retMsgExt.setReconsumeTimes(2);
        retMsgExt.setBornTimestamp(System.currentTimeMillis());
        retMsgExt.setStoreTimestamp(System.currentTimeMillis());
        when(mQAdminImpl.viewMessage(anyString())).thenReturn(retMsgExt);

        when(mQAdminImpl.queryMessageByUniqKey(anyString(), anyString())).thenReturn(retMsgExt);

        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:9876");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        GroupList groupList = new GroupList();
        HashSet<String> groupSets = new HashSet<String>();
        groupSets.add("testGroup");
        groupList.setGroupList(groupSets);
        when(mQClientAPIImpl.queryTopicConsumeByWho(anyString(), anyString(), anyLong())).thenReturn(groupList);


        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setConsumeTps(100*10000);
        HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<MessageQueue, OffsetWrapper>();
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName("messageQueue BrokerName testing");
        messageQueue.setTopic("messageQueue topic");
        messageQueue.setQueueId(1);
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setBrokerOffset(100);
        offsetWrapper.setConsumerOffset(200);
        offsetWrapper.setLastTimestamp(System.currentTimeMillis());
        offsetTable.put(messageQueue, offsetWrapper);
        consumeStats.setOffsetTable(offsetTable);
        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), (String)isNull(), anyLong())).thenReturn(consumeStats);

        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<String, BrokerData>();
        brokerAddrTable.put("key", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<String, Set<String>>();
        Set<String> addrSet = new HashSet<String>();
        addrSet.add("127.0.0.1:9876");
        clusterAddrTable.put("key", addrSet);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        when(mQClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(clusterInfo);

        field = QueryMsgByUniqueKeySubCommand.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);

    }

    @Test
    public void testExecuteConsumeActively() throws SubCommandException, InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection conn = new Connection();
        conn.setClientId("clientIdTest");
        conn.setClientAddr("clientAddrTest");
        conn.setLanguage(LanguageCode.JAVA);
        conn.setVersion(1);
        connectionSet.add(conn);
        consumerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] args = new String[]{"-t myTopicTest", "-i msgId"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

    }

    @Test
    public void testExecuteConsumePassively() throws SubCommandException, InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection conn = new Connection();
        conn.setClientId("clientIdTestStr");
        conn.setClientAddr("clientAddrTestStr");
        conn.setLanguage(LanguageCode.JAVA);
        conn.setVersion(2);
        connectionSet.add(conn);
        consumerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] args = new String[]{"-t myTopicTest", "-i 7F000001000004D20000000000000066"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);



    }

    @Test
    public void testExecuteWithConsumerGroupAndClientId() throws SubCommandException {

        Options options = ServerUtil.buildCommandlineOptions(new Options());

        String[] args = new String[]{"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000", "-g producerGroupName", "-d clientId"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

        System.out.println();
        System.out.println("commandName=" + cmd.commandName());
        System.out.println("commandDesc=" + cmd.commandDesc());

    }

    @Test
    public void testExecute() throws SubCommandException {

        System.setProperty("rocketmq.namesrv.addr", "127.0.0.1:9876");

        QueryMsgByUniqueKeySubCommand cmd = new QueryMsgByUniqueKeySubCommand();
        String[] args = new String[]{"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000"};
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

        args = new String[]{"-t myTopicTest", "-i 0A3A54F7BF7D18B4AAC28A3FA2CF0000", "-g producerGroupName", "-d clientId"};
        commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);

    }
}
