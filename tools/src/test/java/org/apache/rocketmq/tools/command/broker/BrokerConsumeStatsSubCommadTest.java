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
package org.apache.rocketmq.tools.command.broker;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BrokerConsumeStatsSubCommadTest {

    private static BrokerConsumeStatsSubCommad cmd = new BrokerConsumeStatsSubCommad();

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
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

        ConsumeStatsList consumeStatsList = new ConsumeStatsList();
        consumeStatsList.setBrokerAddr("127.0l.0.1:10911");
        consumeStatsList.setConsumeStatsList(new ArrayList<Map<String, List<ConsumeStats>>>());
        consumeStatsList.setTotalDiff(123);
        when(mQClientAPIImpl.fetchConsumeStatsInBroker(anyString(), anyBoolean(), anyLong())).thenReturn(consumeStatsList);
    }

    @AfterClass
    public static void terminate() {
    }

    @Test
    public void testExecute() throws SubCommandException, IllegalAccessException, NoSuchFieldException {

        Field field = BrokerConsumeStatsSubCommad.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-b 127.0.0.1:10911", "-t 3000", "-l 5", "-o true"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }
}
