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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Iterator;
import java.util.Map;

public class ResetOffsetByTimeCommand implements SubCommand {

    @Override
    public String commandName() {
        return "resetOffsetByTime";
    }

    @Override
    public String commandDesc() {
        return "Reset consumer offset by timestamp(without client restart).";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "set broker address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "queue", true, "set the queue, eg: 0,1");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "timestamp", true, "set the timestamp[now|currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "force", true, "set the force rollback by timestamp switch[true|false]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "cplus", false, "reset c++ client offset");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            String timeStampStr = commandLine.getOptionValue("s").trim();
            long timestamp = timeStampStr.equals("now") ? System.currentTimeMillis() : 0;

            try {
                if (timestamp == 0) {
                    timestamp = Long.parseLong(timeStampStr);
                }
            } catch (NumberFormatException e) {

                timestamp = UtilAll.parseDate(timeStampStr, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS).getTime();
            }

            boolean force = true;
            if (commandLine.hasOption('f')) {
                force = Boolean.valueOf(commandLine.getOptionValue("f").trim());
            }

            boolean isC = false;
            if (commandLine.hasOption('c')) {
                isC = true;
            }

            String queueStr = null;
            if (commandLine.hasOption("i")) {
                queueStr = commandLine.getOptionValue("i").trim();
                String[] queues = queueStr.split(",");
                for (int i = 0; i < queues.length; i++) {
                    queues[i] = queues[i].trim();
                    if (Integer.parseInt(queues[i]) < 0) {
                        throw new Exception("queue id must be nonnegative");
                    }
                }
                queueStr = String.join(",", queues);
            }

            String brokerAddr = null;
            if (commandLine.hasOption("b")) {
                brokerAddr = commandLine.getOptionValue("b").trim();
            }

            defaultMQAdminExt.start();
            Map<MessageQueue, Long> offsetTable;
            try {
                if (null != brokerAddr) {
                    offsetTable = defaultMQAdminExt.resetOffsetByTimestampInBroker(brokerAddr, topic, queueStr, group, timestamp, force, isC);
                } else {
                    offsetTable = defaultMQAdminExt.resetOffsetByTimestamp(topic, queueStr, group, timestamp, force, isC);
                }
            } catch (MQClientException e) {
                if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                    ResetOffsetByTimeOldCommand.resetOffset(defaultMQAdminExt, group, topic, queueStr, timestamp, force, timeStampStr);
                    return;
                }
                throw e;
            }

            System.out.printf("rollback consumer offset by specified group[%s], topic[%s], queue[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]%n",
                group, topic, queueStr, force, timeStampStr, timestamp);

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
