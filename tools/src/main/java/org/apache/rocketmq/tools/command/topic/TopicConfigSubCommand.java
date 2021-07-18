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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class TopicConfigSubCommand implements SubCommand {
    private static final String FORMAT = "%-15s  %-15s  %-10s %15s %15s %5s %15s %15s %6s%n";

    @Override
    public String commandName() {
        return "topicConfig";
    }

    @Override
    public String commandDesc() {
        return "Examine topic config info";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String topic = commandLine.getOptionValue('t').trim();

            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);

            System.out.printf(FORMAT,
                    "#Cluster Name",
                    "#Broker Name",
                    "#Topic Name",
                    "#ReadQueueNums",
                    "#WriteQueueNums",
                    "#Perm",
                    "#FilterType",
                    "#TopicSysFlag",
                    "#Order"
            );
            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                TopicConfig topicConfig = defaultMQAdminExt.examineTopicConfig(bd.getBrokerAddrs().get(0L), topic);
                System.out.printf(FORMAT,
                        bd.getCluster(),
                        bd.getBrokerName(),
                        topicConfig.getTopicName(),
                        topicConfig.getReadQueueNums(),
                        topicConfig.getWriteQueueNums(),
                        PermName.perm2String(topicConfig.getPerm()),
                        topicConfig.getTopicFilterType(),
                        topicConfig.getTopicSysFlag(),
                        topicConfig.isOrder()
                );
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
