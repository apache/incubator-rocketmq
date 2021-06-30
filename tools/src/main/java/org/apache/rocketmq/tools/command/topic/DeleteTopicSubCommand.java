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
package org.apache.rocketmq.tools.command.topic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class DeleteTopicSubCommand implements SubCommand {
    public static void deleteTopic(final DefaultMQAdminExt adminExt,
        final String clusterName,
        final String topic
    ) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {

        Set<String> brokerAddressSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(adminExt, clusterName);
        adminExt.deleteTopicInBroker(brokerAddressSet, topic);
        System.out.printf("delete topic [%s] from cluster [%s] success.%n", topic, clusterName);

        Set<String> nameServerSet = null;
        if (adminExt.getNamesrvAddr() != null) {
            String[] ns = adminExt.getNamesrvAddr().trim().split(";");
            nameServerSet = new HashSet(Arrays.asList(ns));
        }

        adminExt.deleteTopicInNameServer(nameServerSet, topic, null);
        System.out.printf("delete topic [%s] from NameServer success.%n", topic);
    }

    public static void deleteTopicFromBroker(final DefaultMQAdminExt adminExt,
        final String brokerAddr,
        final String topic
    ) throws Exception {

        adminExt.deleteTopicInBroker(new HashSet<String>(Arrays.asList(brokerAddr)), topic);
        System.out.printf("delete topic [%s] from broker [%s] success.%n", topic, brokerAddr);

        Set<String> nameServerSet = null;
        if (adminExt.getNamesrvAddr() != null) {
            String[] ns = adminExt.getNamesrvAddr().trim().split(";");
            nameServerSet = new HashSet<String>(Arrays.asList(ns));
        }

        String brokerName = CommandUtil.fetchBrokerNameByAddr(adminExt, brokerAddr);
        adminExt.deleteTopicInNameServer(nameServerSet, topic, brokerName);
        System.out.printf("delete topic [%s] of [%s] from NameServer success.%n", topic, brokerName);
    }

    @Override
    public String commandName() {
        return "deleteTopic";
    }

    @Override
    public String commandDesc() {
        return "Delete topic from broker and NameServer.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete topic from which cluster");
        optionGroup.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "delete topic from which broker, eg: 192.168.0.1:10911");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String topic = commandLine.getOptionValue('t').trim();

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                adminExt.start();
                deleteTopicFromBroker(adminExt, brokerAddr, topic);
                return;
            }

            if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                adminExt.start();
                deleteTopic(adminExt, clusterName, topic);
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
