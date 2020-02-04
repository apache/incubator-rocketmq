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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ConsumerProgressSubCommand implements SubCommand {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public String commandName() {
        return "consumerProgress";
    }

    @Override
    public String commandDesc() {
        return "Query consumers's progress, speed";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        Option optionShowClientIP = new Option("s", "showClientIP", true, "Show Client IP per Queue");
        optionShowClientIP.setRequired(false);
        options.addOption(optionShowClientIP);

        return options;
    }

    private Map<MessageQueue, String> getMessageQueueAllocationResult(DefaultMQAdminExt defaultMQAdminExt,
        String groupName) {
        Map<MessageQueue, String> results = new HashMap<>();
        try {
            ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName);
            for (Connection connection : consumerConnection.getConnectionSet()) {
                String clientId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(groupName, clientId,
                    false);
                for (MessageQueue messageQueue : consumerRunningInfo.getMqTable().keySet()) {
                    results.put(messageQueue, clientId.split("@")[0]);
                }
            }
        } catch (Exception ignore) {
        }
        return results;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            boolean showClientIP = commandLine.hasOption('s')
                && "true".equalsIgnoreCase(commandLine.getOptionValue('s'));

            if (commandLine.hasOption('g')) {
                String consumerGroup = commandLine.getOptionValue('g').trim();
                ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);
                List<MessageQueue> mqList = new LinkedList<MessageQueue>();
                mqList.addAll(consumeStats.getOffsetTable().keySet());
                Collections.sort(mqList);

                Map<MessageQueue, String> messageQueueAllocationResult = null;
                if (showClientIP) {
                    messageQueueAllocationResult = getMessageQueueAllocationResult(defaultMQAdminExt, consumerGroup);
                }

                System.out.printf("%-32s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s  %s%n",
                    "#Topic",
                    "#Broker Name",
                    "#QID",
                    "#Broker Offset",
                    "#Consumer Offset",
                    "#Client IP",
                    "#Diff",
                    "#LastTime");

                long diffTotal = 0L;
                for (MessageQueue mq : mqList) {
                    OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);
                    long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
                    diffTotal += diff;
                    String lastTime = "";
                    try {
                        if (offsetWrapper.getLastTimestamp() == 0) {
                            lastTime = "N/A";
                        } else {
                            lastTime = UtilAll.formatDate(new Date(offsetWrapper.getLastTimestamp()), UtilAll.YYYY_MM_DD_HH_MM_SS);
                        }
                    } catch (Exception e) {
                    }

                    String clientIP = null;
                    if (showClientIP) {
                        clientIP = messageQueueAllocationResult.get(mq);
                    }

                    System.out.printf("%-32s  %-32s  %-4d  %-20d  %-20d  %-20s %-20d  %s%n",
                        UtilAll.frontStringAtLeast(mq.getTopic(), 32),
                        UtilAll.frontStringAtLeast(mq.getBrokerName(), 32),
                        mq.getQueueId(),
                        offsetWrapper.getBrokerOffset(),
                        offsetWrapper.getConsumerOffset(),
                        null != clientIP ? clientIP : "N/A",
                        diff,
                        lastTime
                    );
                }

                System.out.printf("%n");
                System.out.printf("Consume TPS: %.2f%n", consumeStats.getConsumeTps());
                System.out.printf("Diff Total: %d%n", diffTotal);
            } else {
                System.out.printf("%-32s  %-6s  %-24s %-5s  %-14s  %-7s  %s%n",
                    "#Group",
                    "#Count",
                    "#Version",
                    "#Type",
                    "#Model",
                    "#TPS",
                    "#Diff Total"
                );
                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        String consumerGroup = topic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
                        try {
                            ConsumeStats consumeStats = null;
                            try {
                                consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);
                            } catch (Exception e) {
                                log.warn("examineConsumeStats exception, " + consumerGroup, e);
                            }

                            ConsumerConnection cc = null;
                            try {
                                cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
                            } catch (Exception e) {
                                log.warn("examineConsumerConnectionInfo exception, " + consumerGroup, e);
                            }

                            GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
                            groupConsumeInfo.setGroup(consumerGroup);

                            if (consumeStats != null) {
                                groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                                groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
                            }

                            if (cc != null) {
                                groupConsumeInfo.setCount(cc.getConnectionSet().size());
                                groupConsumeInfo.setMessageModel(cc.getMessageModel());
                                groupConsumeInfo.setConsumeType(cc.getConsumeType());
                                groupConsumeInfo.setVersion(cc.computeMinVersion());
                            }

                            System.out.printf("%-32s  %-6d  %-24s %-5s  %-14s  %-7d  %d%n",
                                UtilAll.frontStringAtLeast(groupConsumeInfo.getGroup(), 32),
                                groupConsumeInfo.getCount(),
                                groupConsumeInfo.getCount() > 0 ? groupConsumeInfo.versionDesc() : "OFFLINE",
                                groupConsumeInfo.consumeTypeDesc(),
                                groupConsumeInfo.messageModelDesc(),
                                groupConsumeInfo.getConsumeTps(),
                                groupConsumeInfo.getDiffTotal()
                            );
                        } catch (Exception e) {
                            log.warn("examineConsumeStats or examineConsumerConnectionInfo exception, " + consumerGroup, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}

class GroupConsumeInfo implements Comparable<GroupConsumeInfo> {
    private String group;
    private int version;
    private int count;
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private int consumeTps;
    private long diffTotal;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String consumeTypeDesc() {
        if (this.count != 0) {
            return this.getConsumeType() == ConsumeType.CONSUME_ACTIVELY ? "PULL" : "PUSH";
        }
        return "";
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public String messageModelDesc() {
        if (this.count != 0 && this.getConsumeType() == ConsumeType.CONSUME_PASSIVELY) {
            return this.getMessageModel().toString();
        }
        return "";
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String versionDesc() {
        if (this.count != 0) {
            return MQVersion.getVersionDesc(this.version);
        }
        return "";
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getDiffTotal() {
        return diffTotal;
    }

    public void setDiffTotal(long diffTotal) {
        this.diffTotal = diffTotal;
    }

    @Override
    public int compareTo(GroupConsumeInfo o) {
        if (this.count != o.count) {
            return o.count - this.count;
        }

        return (int) (o.diffTotal - diffTotal);
    }

    public int getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(int consumeTps) {
        this.consumeTps = consumeTps;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
