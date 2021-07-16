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
package org.apache.rocketmq.tools.command.offset;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class SkipAccumulationSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "skipAccumulatedMessage";
    }

    @Override
    public String commandDesc() {
        return "Skip all messages that are accumulated (not consumed) currently";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "force", true, "set the force rollback by timestamp switch[true|false]");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        long timestamp = -1;
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            boolean force = true;
            if (commandLine.hasOption('f')) {
                force = Boolean.valueOf(commandLine.getOptionValue("f").trim());
            }

            defaultMQAdminExt.start();
            Map<MessageQueue, Long> offsetTable;
            try {
                offsetTable = defaultMQAdminExt.resetOffsetByTimestamp(topic, group, timestamp, force);
            } catch (MQClientException e) {
                if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                    List<RollbackStats> rollbackStatsList = defaultMQAdminExt.resetOffsetByTimestampOld(group, topic, timestamp, force);
                    System.out.printf("%-20s  %-20s  %-20s  %-20s  %-20s  %-20s%n",
                        "#brokerName",
                        "#queueId",
                        "#brokerOffset",
                        "#consumerOffset",
                        "#timestampOffset",
                        "#rollbackOffset"
                    );

                    for (RollbackStats rollbackStats : rollbackStatsList) {
                        System.out.printf("%-20s  %-20d  %-20d  %-20d  %-20d  %-20d%n",
                            UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(), 32),
                            rollbackStats.getQueueId(),
                            rollbackStats.getBrokerOffset(),
                            rollbackStats.getConsumerOffset(),
                            rollbackStats.getTimestampOffset(),
                            rollbackStats.getRollbackOffset()
                        );
                    }
                    return;
                }
                throw e;
            }

            System.out.printf("%-40s  %-40s  %-40s%n",
                "#brokerName",
                "#queueId",
                "#offset");

            Iterator<Map.Entry<MessageQueue, Long>> iterator = offsetTable.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<MessageQueue, Long> entry = iterator.next();
                System.out.printf("%-40s  %-40d  %-40d%n",
                    UtilAll.frontStringAtLeast(entry.getKey().getBrokerName(), 32),
                    entry.getKey().getQueueId(),
                    entry.getValue());
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
