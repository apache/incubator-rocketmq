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
package org.apache.rocketmq.tools.command.acl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CommandUtil.class)
public class UpdateAccessConfigSubCommandTest {
    private static UpdateAccessConfigSubCommand cmd = new UpdateAccessConfigSubCommand();
    private static DefaultMQAdminExt defaultMQAdminExt;

    @BeforeClass
    public static void init() throws Exception {
        defaultMQAdminExt = mock(DefaultMQAdminExt.class);
        Field field = UpdateAccessConfigSubCommand.class.getField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);
    }

    @Test
    public void testExecuteWithOptionBroker() throws SubCommandException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{
                "-b 127.0.0.1:10911",
                "-a RocketMQ",
                "-s 12345678",
                "-w 192.168.0.*",
                "-i DENY",
                "-u SUB",
                "-t topicA=DENY;topicB=PUB|SUB",
                "-g groupA=DENY;groupB=SUB",
                "-m true"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        assertThat(commandLine.getOptionValue('b').trim()).isEqualTo("127.0.0.1:10911");
        assertThat(commandLine.getOptionValue('a').trim()).isEqualTo("RocketMQ");
        assertThat(commandLine.getOptionValue('s').trim()).isEqualTo("12345678");
        assertThat(commandLine.getOptionValue('w').trim()).isEqualTo("192.168.0.*");
        assertThat(commandLine.getOptionValue('i').trim()).isEqualTo("DENY");
        assertThat(commandLine.getOptionValue('u').trim()).isEqualTo("SUB");
        assertThat(commandLine.getOptionValue('t').trim()).isEqualTo("topicA=DENY;topicB=PUB|SUB");
        assertThat(commandLine.getOptionValue('g').trim()).isEqualTo("groupA=DENY;groupB=SUB");
        assertThat(commandLine.getOptionValue('m').trim()).isEqualTo("true");

        PlainAccessConfig accessConfig = new PlainAccessConfig();

        // topicPerms list value
        if (commandLine.hasOption('t')) {
            String[] topicPerms = commandLine.getOptionValue('t').trim().split(";");
            List<String> topicPermList = new ArrayList<String>();
            if (topicPerms != null) {
                for (String topicPerm : topicPerms) {
                    topicPermList.add(topicPerm);
                }
            }
            accessConfig.setTopicPerms(topicPermList);
        }

        // groupPerms list value
        if (commandLine.hasOption('g')) {
            String[] groupPerms = commandLine.getOptionValue('g').trim().split(";");
            List<String> groupPermList = new ArrayList<String>();
            if (groupPerms != null) {
                for (String groupPerm : groupPerms) {
                    groupPermList.add(groupPerm);
                }
            }
            accessConfig.setGroupPerms(groupPermList);
        }

        Assert.assertTrue(accessConfig.getTopicPerms().contains("topicB=PUB|SUB"));
        Assert.assertTrue(accessConfig.getGroupPerms().contains("groupB=SUB"));

        cmd.execute(commandLine, options, null);
        verify(defaultMQAdminExt, times(1)).createAndUpdatePlainAccessConfig(eq("127.0.0.1:10911"), any());
    }

    @Test
    public void testExecuteWithOptionCluster() throws Exception {
        String clusterName = "defaultTestCluster";
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{
                "-c " + clusterName,
                "-a RocketMQ",
                "-s 12345678",
                "-w 192.168.0.*",
                "-i DENY",
                "-u SUB",
                "-t topicA=DENY;topicB=PUB|SUB",
                "-g groupA=DENY;groupB=SUB",
                "-m true"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        Set<String> masterAndSlaveAddrSet = new HashSet<>();
        masterAndSlaveAddrSet.add("1.1.1.1:10911");
        masterAndSlaveAddrSet.add("1.1.1.2:10911");
        PowerMockito.mockStatic(CommandUtil.class);
        PowerMockito.when(CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName)).thenReturn(masterAndSlaveAddrSet);
        cmd.execute(commandLine, options, null);
        verify(defaultMQAdminExt, times(1)).createAndUpdatePlainAccessConfig(eq("1.1.1.1:10911"), any());
        verify(defaultMQAdminExt, times(1)).createAndUpdatePlainAccessConfig(eq("1.1.1.2:10911"), any());
    }

    @Test(expected = SubCommandException.class)
    public void testExecuteWithException() throws Exception {
        String clusterName = "defaultTestCluster";
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{
                "-c " + clusterName,
                "-a RocketMQ",
                "-s 12345678",
                "-w 192.168.0.*",
                "-i DENY",
                "-u SUB",
                "-t topicA=DENY;topicB=PUB|SUB",
                "-g groupA=DENY;groupB=SUB",
                "-m true"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
        doThrow(new Exception()).when(defaultMQAdminExt).createAndUpdatePlainAccessConfig(any(), any());
    }

}
