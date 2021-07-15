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

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.MigrateLogicalQueueBody;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;

public class MigrateTopicLogicalQueueCommand implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STDOUT_LOGGER_NAME);

    @Override public String commandName() {
        return "migrateTopicLogicalQueue";
    }

    @Override public String commandDesc() {
        return "migrate a logical queue of a topic from one broker to another.";
    }

    @Override public Options buildCommandlineOptions(Options options) {
        Option opt;

        opt = new Option("t", "topic", true, "topic name.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "index", true, "logical queue index.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "new broker name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("fd", "forceDelta", true, "assume fromBroker down, force migrate");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public void execute(DefaultMQAdminExt mqAdminExt, String topic, int logicalQueueIdx,
        String toBrokerName,
        Long forceDelta) throws RemotingException, MQBrokerException, InterruptedException, SubCommandException, MQClientException {
        TopicRouteData topicRouteInfo = mqAdminExt.examineTopicRouteInfo(topic);
        LogicalQueuesInfo logicalQueuesInfo = topicRouteInfo.getLogicalQueuesInfo();
        if (logicalQueuesInfo == null) {
            throw new SubCommandException("topic not enabled logical queue");
        }
        List<LogicalQueueRouteData> queueRouteDataList = logicalQueuesInfo.get(logicalQueueIdx);
        if (queueRouteDataList == null) {
            throw new SubCommandException("logical queue %d not exist", logicalQueueIdx);
        }
        Map<String, BrokerData> brokerAddrTable = mqAdminExt.examineBrokerClusterInfo().getBrokerAddrTable();
        String toBrokerAddr = lookupBrokerMasterAddr(brokerAddrTable, toBrokerName);
        if (toBrokerAddr == null) {
            throw new SubCommandException("destination broker %s not found", toBrokerName);
        }
        LogicalQueueRouteData fromQueueRouteData = queueRouteDataList.stream().filter(LogicalQueueRouteData::isWritable).reduce((first, second) -> second).orElse(null);
        if (fromQueueRouteData == null) {
            throw new SubCommandException("logical queue %d not writable, can not migrate", logicalQueueIdx);
        }
        String fromBrokerName = fromQueueRouteData.getBrokerName();
        String fromBrokerAddr = ofNullable(lookupBrokerMasterAddr(brokerAddrTable, fromBrokerName)).orElse(fromQueueRouteData.getBrokerAddr());
        if (fromBrokerAddr == null) {
            throw new SubCommandException("unexpected source broker name %s not found", fromBrokerName);
        }
        LogicalQueueRouteData toQueueRouteData;
        RETRY:
        while (true) {
            TopicConfig topicConfig = mqAdminExt.examineTopicConfig(toBrokerAddr, topic);
            LogicalQueuesInfo logicalQueuesInfoInBroker = ofNullable(mqAdminExt.queryTopicLogicalQueueMapping(toBrokerAddr, topic)).orElse(new LogicalQueuesInfo());
            toQueueRouteData = logicalQueuesInfoInBroker.getOrDefault(logicalQueueIdx, Collections.emptyList()).stream().filter(queueRouteData -> Objects.equals(toBrokerName, queueRouteData.getBrokerName()) && queueRouteData.isWriteOnly()).findFirst().orElse(null);
            if (toQueueRouteData == null) {
                Multimap<Integer, LogicalQueueRouteData> m = Multimaps.index(logicalQueuesInfoInBroker.values().stream().flatMap(Collection::stream).filter(queueRouteData -> Objects.equals(toBrokerName, queueRouteData.getBrokerName())).iterator(), LogicalQueueRouteData::getQueueId);
                for (int queueId = 0, writeQueueNums = topicConfig.getWriteQueueNums(); queueId < writeQueueNums; queueId++) {
                    if (m.get(queueId).stream().anyMatch(LogicalQueueRouteData::isWritable)) {
                        continue;
                    }
                    try {
                        toQueueRouteData = mqAdminExt.reuseTopicLogicalQueue(toBrokerAddr, topic, queueId, logicalQueueIdx, MessageQueueRouteState.WriteOnly);
                        log.info("reuseTopicLogicalQueue brokerName={} brokerAddr={} queueId={} logicalQueueIdx={} ok: {}", toBrokerName, toBrokerAddr, queueId, logicalQueueIdx, toQueueRouteData);
                        break;
                    } catch (MQBrokerException e) {
                        if ("queue writable".equals(e.getErrorMessage())) {
                            log.info("reuseTopicLogicalQueue brokerName={} brokerAddr={} queueId={} logicalQueueIdx={} writable, try again.", toBrokerName, toBrokerAddr, queueId, logicalQueueIdx);
                            continue RETRY;
                        } else {
                            throw e;
                        }
                    }
                }
            }
            break;
        }
        if (toQueueRouteData == null) {
            toQueueRouteData = mqAdminExt.createMessageQueueForLogicalQueue(toBrokerAddr, topic, logicalQueueIdx, MessageQueueRouteState.WriteOnly);
            log.info("createMessageQueueForLogicalQueue brokerName={} brokerAddr={} logicalQueueIdx={} ok: {}", toBrokerName, toBrokerAddr, logicalQueueIdx, toQueueRouteData);
        }
        MigrateLogicalQueueBody migrateLogicalQueueBody;
        if (forceDelta == null) {
            try {
                migrateLogicalQueueBody = mqAdminExt.migrateTopicLogicalQueuePrepare(fromQueueRouteData, toQueueRouteData);
            } catch (RemotingConnectException e) {
                throw new SubCommandException("migrateTopicLogicalQueuePrepare", e);
            }
            fromQueueRouteData = migrateLogicalQueueBody.getFromQueueRouteData();
            toQueueRouteData = migrateLogicalQueueBody.getToQueueRouteData();
            log.info("migrateTopicLogicalQueuePrepare from {} to {}", fromQueueRouteData, toQueueRouteData);
        } else {
            toQueueRouteData.setLogicalQueueDelta(forceDelta);
            log.warn("migrateTopicLogicalQueuePrepare skip with forceDelta={}", forceDelta);
        }
        migrateLogicalQueueBody = mqAdminExt.migrateTopicLogicalQueueCommit(fromQueueRouteData, toQueueRouteData);
        toQueueRouteData = migrateLogicalQueueBody.getToQueueRouteData();
        log.info("migrateTopicLogicalQueueCommit got: {}", toQueueRouteData);
        if (forceDelta == null) {
            try {
                mqAdminExt.migrateTopicLogicalQueueNotify(fromBrokerAddr, fromQueueRouteData, toQueueRouteData);
            } finally {
                log.info("migrateTopicLogicalQueueNotify fromBroker {} {}", fromQueueRouteData.getBrokerName(), fromBrokerAddr);
            }
        }
        Collection<String> ignoreBrokerNames = Arrays.asList(fromBrokerName, toBrokerName);
        Set<String> brokerNames = queueRouteDataList.stream()
            .map(LogicalQueueRouteData::getBrokerName)
            .filter(v -> !ignoreBrokerNames.contains(v))
            .map(v -> lookupBrokerMasterAddr(brokerAddrTable, v))
            .collect(Collectors.toSet());
        int i = 1;
        for (String brokerName : brokerNames) {
            String brokerAddr = null;
            try {
                brokerAddr = lookupBrokerMasterAddr(brokerAddrTable, brokerName);
                mqAdminExt.migrateTopicLogicalQueueNotify(brokerAddr, fromQueueRouteData, toQueueRouteData);
            } finally {
                log.info("migrateTopicLogicalQueueNotify otherBroker {}({}}) ({}/{})", brokerName, brokerAddr, i, brokerNames.size());
            }
        }
    }

    @Override public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(rpcHook);
        mqAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String topic = commandLine.getOptionValue("t").trim();
            String newBrokerName = commandLine.getOptionValue("b").trim();
            int logicalQueueIdx = Integer.parseInt(commandLine.getOptionValue("i").trim());
            Long forceDelta = null;
            if (commandLine.hasOption("fd")) {
                forceDelta = Long.parseLong(commandLine.getOptionValue("fd").trim());
            }
            execute(mqAdminExt, topic, logicalQueueIdx, newBrokerName, forceDelta);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            mqAdminExt.shutdown();
        }
    }

    private static String lookupBrokerMasterAddr(Map<String, BrokerData> brokerAddrTable, String brokerName) {
        return ofNullable(brokerAddrTable.get(brokerName)).map(BrokerData::selectBrokerAddr).orElse(null);
    }
}
