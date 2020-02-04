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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateAccessConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateAclConfig";
    }

    @Override
    public String commandDesc() {
        return "Update acl config yaml file in broker";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "update acl config file to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "update cl config file to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("a", "accessKey", true, "set accessKey in acl config file");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "secretKey", true, "set secretKey in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "whiteRemoteAddress", true, "set white ip Address for account in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "defaultTopicPerm", true, "set default topicPerm in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "defaultGroupPerm", true, "set default GroupPerm in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topicPerms", true, "set topicPerms list,eg: topicA=DENY,topicD=SUB");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupPerms", true, "set groupPerms list,eg: groupD=DENY,groupD=SUB");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "admin", true, "set admin flag in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            PlainAccessConfig accessConfig = new PlainAccessConfig();
            accessConfig.setAccessKey(commandLine.getOptionValue('a').trim());
            // Secretkey
            if (commandLine.hasOption('s')) {
                accessConfig.setSecretKey(commandLine.getOptionValue('s').trim());
            }

            // Admin
            if (commandLine.hasOption('m')) {
                accessConfig.setAdmin(Boolean.parseBoolean(commandLine.getOptionValue('m').trim()));
            }

            // DefaultTopicPerm
            if (commandLine.hasOption('i')) {
                accessConfig.setDefaultTopicPerm(commandLine.getOptionValue('i').trim());
            }

            // DefaultGroupPerm
            if (commandLine.hasOption('u')) {
                accessConfig.setDefaultGroupPerm(commandLine.getOptionValue('u').trim());
            }

            // WhiteRemoteAddress
            if (commandLine.hasOption('w')) {
                accessConfig.setWhiteRemoteAddress(commandLine.getOptionValue('w').trim());
            }

            // TopicPerms list value
            if (commandLine.hasOption('t')) {
                String[] topicPerms = commandLine.getOptionValue('t').trim().split(",");
                List<String> topicPermList = new ArrayList<String>();
                if (topicPerms != null) {
                    for (String topicPerm : topicPerms) {
                        topicPermList.add(topicPerm);
                    }
                }
                accessConfig.setTopicPerms(topicPermList);
            }

            // GroupPerms list value
            if (commandLine.hasOption('g')) {
                String[] groupPerms = commandLine.getOptionValue('g').trim().split(",");
                List<String> groupPermList = new ArrayList<String>();
                if (groupPerms != null) {
                    for (String groupPerm : groupPerms) {
                        groupPermList.add(groupPerm);
                    }
                }
                accessConfig.setGroupPerms(groupPermList);
            }

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdatePlainAccessConfig(addr, accessConfig);

                System.out.printf("create or update plain access config to %s success.%n", addr);
                System.out.printf("%s", accessConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdatePlainAccessConfig(addr, accessConfig);
                    System.out.printf("create or update plain access config to %s success.%n", addr);
                }

                System.out.printf("%s", accessConfig);
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
