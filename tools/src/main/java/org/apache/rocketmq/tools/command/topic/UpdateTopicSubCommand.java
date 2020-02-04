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

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateTopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopic";
    }

    @Override
    public String commandDesc() {
        return "Update or create topic";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "readQueueNums", true, "set read queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "writeQueueNums", true, "set write queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(2|4|6), intro[2:W 4:R; 6:RW]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "order", true, "set topic's order(true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "unit", true, "is unit topic (true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "hasUnitSub", true, "has unit sub (true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(commandLine.getOptionValue('t').trim());

            // readQueueNums
            if (commandLine.hasOption('r')) {
                topicConfig.setReadQueueNums(Integer.parseInt(commandLine.getOptionValue('r').trim()));
            }

            // writeQueueNums
            if (commandLine.hasOption('w')) {
                topicConfig.setWriteQueueNums(Integer.parseInt(commandLine.getOptionValue('w').trim()));
            }

            // perm
            if (commandLine.hasOption('p')) {
                topicConfig.setPerm(Integer.parseInt(commandLine.getOptionValue('p').trim()));
            }

            boolean isUnit = false;
            if (commandLine.hasOption('u')) {
                isUnit = Boolean.parseBoolean(commandLine.getOptionValue('u').trim());
            }

            boolean isCenterSync = false;
            if (commandLine.hasOption('s')) {
                isCenterSync = Boolean.parseBoolean(commandLine.getOptionValue('s').trim());
            }

            int topicCenterSync = TopicSysFlag.buildSysFlag(isUnit, isCenterSync);
            topicConfig.setTopicSysFlag(topicCenterSync);

            boolean isOrder = false;
            if (commandLine.hasOption('o')) {
                isOrder = Boolean.parseBoolean(commandLine.getOptionValue('o').trim());
            }
            topicConfig.setOrder(isOrder);

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);

                if (isOrder) {
                    String brokerName = CommandUtil.fetchBrokerNameByAddr(defaultMQAdminExt, addr);
                    String orderConf = brokerName + ":" + topicConfig.getWriteQueueNums();
                    defaultMQAdminExt.createOrUpdateOrderConf(topicConfig.getTopicName(), orderConf, false);
                    System.out.printf("%s", String.format("set broker orderConf. isOrder=%s, orderConf=[%s]",
                        isOrder, orderConf.toString()));
                }
                System.out.printf("create topic to %s success.%n", addr);
                System.out.printf("%s", topicConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                    System.out.printf("create topic to %s success.%n", addr);
                }

                if (isOrder) {
                    Set<String> brokerNameSet =
                        CommandUtil.fetchBrokerNameByClusterName(defaultMQAdminExt, clusterName);
                    StringBuilder orderConf = new StringBuilder();
                    String splitor = "";
                    for (String s : brokerNameSet) {
                        orderConf.append(splitor).append(s).append(":")
                            .append(topicConfig.getWriteQueueNums());
                        splitor = ";";
                    }
                    defaultMQAdminExt.createOrUpdateOrderConf(topicConfig.getTopicName(),
                        orderConf.toString(), true);
                    System.out.printf("set cluster orderConf. isOrder=%s, orderConf=[%s]", isOrder, orderConf);
                }

                System.out.printf("%s", topicConfig);
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
