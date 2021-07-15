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
package org.apache.rocketmq.tools.command.logicalqueue;

import com.google.common.collect.Maps;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateTopicLogicalQueueMappingCommand implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STDOUT_LOGGER_NAME);

    @Override public String commandName() {
        return "updateTopicLogicalQueueMapping";
    }

    @Override public String commandDesc() {
        return "update logical queue mapping info of a topic";
    }

    @Override public Options buildCommandlineOptions(Options options) {
        Option opt;

        opt = new Option("t", "topic", true, "topic name.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("q", "queue", true, "message queue id.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "index", true, "logical queue index.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "broker addr.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "cluster name.");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public void execute(DefaultMQAdminExt defaultMQAdminExt, String topic, Collection<String> brokerAddrs) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        Map<String, TopicConfig> topicConfigMap = Maps.newHashMapWithExpectedSize(brokerAddrs.size());
        Map<String, BitSet> allocatedMessageQueueMap = Maps.newHashMap();
        BitSet allocatedLogicalQueueIndices = new BitSet();
        brokerAddrs = brokerAddrs.stream().sorted().collect(Collectors.toList());
        for (String brokerAddr : brokerAddrs) {
            TopicConfig topicConfig = defaultMQAdminExt.examineTopicConfig(brokerAddr, topic);
            if (topicConfig == null) {
                log.warn("examineTopicConfig brokerAddr={} topic={} not exist, skip!", brokerAddr, topic);
                continue;
            }
            topicConfigMap.put(brokerAddr, topicConfig);

            BitSet allocatedQueueIds = new BitSet();
            Optional.ofNullable(defaultMQAdminExt.queryTopicLogicalQueueMapping(brokerAddr, topic))
                .ifPresent(queueRouteData -> queueRouteData.forEach((idx, value) -> {
                    allocatedLogicalQueueIndices.set(idx);
                    value.stream().mapToInt(LogicalQueueRouteData::getQueueId).forEach(allocatedQueueIds::set);
                }));
            allocatedMessageQueueMap.put(brokerAddr, allocatedQueueIds);
        }
        int unallocatedLogicalQueueIdx = -1;
        for (Map.Entry<String, TopicConfig> entry : topicConfigMap.entrySet()) {
            String brokerAddr = entry.getKey();
            TopicConfig topicConfig = entry.getValue();
            int queueNums = Integer.max(topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums());
            BitSet allocatedQueueIds = allocatedMessageQueueMap.get(brokerAddr);
            for (int unallocatedQueueId = allocatedQueueIds.nextClearBit(0); unallocatedQueueId < queueNums; unallocatedQueueId = allocatedQueueIds.nextClearBit(unallocatedQueueId + 1)) {
                unallocatedLogicalQueueIdx = allocatedLogicalQueueIndices.nextClearBit(unallocatedLogicalQueueIdx + 1);
                log.info("updateTopicLogicalQueueMapping brokerAddr={} topic={} queueId={} to {}", brokerAddr, topic, unallocatedQueueId, unallocatedLogicalQueueIdx);
                defaultMQAdminExt.updateTopicLogicalQueueMapping(brokerAddr, topic, unallocatedQueueId, unallocatedLogicalQueueIdx);
                allocatedQueueIds.set(unallocatedQueueId);
                allocatedLogicalQueueIndices.set(unallocatedLogicalQueueIdx);
            }
        }
    }

    @Override public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue("t").trim();
            List<String> brokerAddrs;
            if (commandLine.hasOption("b")) {
                String brokerAddr = commandLine.getOptionValue("b").trim();
                boolean hasQueueId = commandLine.hasOption("q");
                boolean hasLogicalQueueIndex = commandLine.hasOption("i");
                if (hasQueueId && hasLogicalQueueIndex) {
                    int queueId = Integer.parseInt(commandLine.getOptionValue("q").trim());
                    int logicalQueueIndex = Integer.parseInt(commandLine.getOptionValue("i").trim());
                    defaultMQAdminExt.updateTopicLogicalQueueMapping(brokerAddr, topic, queueId, logicalQueueIndex);
                    log.info("updateTopicLogicalQueueMapping brokerAddr={} topic={} queueId={} to {}", brokerAddr, topic, queueId, logicalQueueIndex);
                    return;
                } else if (hasQueueId || hasLogicalQueueIndex) {
                    log.error("logicalQueueIndex and queueId must be specified together.");
                    return;
                } else {
                    log.error("brokerAddr specified but no logicalQueueIndex and queueId found");
                    return;
                }
            } else if (commandLine.hasOption("c")) {
                String clusterName = commandLine.getOptionValue("c").trim();
                brokerAddrs = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName).stream().sorted().collect(Collectors.toList());
            } else {
                ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
                return;
            }
            this.execute(defaultMQAdminExt, topic, brokerAddrs);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
