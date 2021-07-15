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

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateTopicLogicalQueueNumCommand implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STDOUT_LOGGER_NAME);

    @Override public String commandName() {
        return "updateTopicLogicalQueueNum";
    }

    @Override public String commandDesc() {
        return "change logical queue num (increase or decrease) of a topic.";
    }

    @Override public Options buildCommandlineOptions(Options options) {
        Option opt;

        opt = new Option("t", "topic", true, "topic name.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "cluster name.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("n", "num", true, "logical queue num.");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            String clusterName = commandLine.getOptionValue("c").trim();
            String topic = commandLine.getOptionValue("t").trim();
            int newLogicalQueueNum = Integer.parseUnsignedInt(commandLine.getOptionValue("n"));
            execute(defaultMQAdminExt, clusterName, topic, newLogicalQueueNum);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    public void execute(DefaultMQAdminExt defaultMQAdminExt, String clusterName, String topic,
        int newLogicalQueueNum) throws RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException, MQBrokerException, InterruptedException, SubCommandException {
        List<String> brokerAddrs = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName).stream().sorted().collect(Collectors.toList());
        Map<String, TopicConfig> topicConfigsByBrokerAddr = Maps.newHashMapWithExpectedSize(brokerAddrs.size());
        NavigableMap<Integer, List<LogicalQueueRouteData>> allLogicalQueueMapByIndex = Maps.newTreeMap();
        Map<String, LogicalQueuesInfo> allLogicalQueueMapByBroker = Maps.newHashMap();
        for (String brokerAddr : brokerAddrs) {
            TopicConfig topicConfig = defaultMQAdminExt.examineTopicConfig(brokerAddr, topic);
            if (topicConfig == null) {
                log.info("examineTopicConfig brokerAddr={} topic={} not exist, skip!", brokerAddr, topic);
                continue;
            }
            topicConfigsByBrokerAddr.put(brokerAddr, topicConfig);

            LogicalQueuesInfo logicalQueuesInfo = defaultMQAdminExt.queryTopicLogicalQueueMapping(brokerAddr, topic);
            if (logicalQueuesInfo == null) {
                throw new SubCommandException(String.format(Locale.ENGLISH, "broker=%s topic=%s logical queue not enabled", brokerAddr, topic));
            }
            allLogicalQueueMapByBroker.put(brokerAddr, logicalQueuesInfo);
            logicalQueuesInfo.values().stream().flatMap(Collection::stream).forEach(queueRouteData -> {
                List<LogicalQueueRouteData> sortedQueueRouteDataList = allLogicalQueueMapByIndex.computeIfAbsent(queueRouteData.getLogicalQueueIndex(), ignore -> Lists.newArrayListWithExpectedSize(1));
                int idx = Collections.binarySearch(sortedQueueRouteDataList, queueRouteData,
                    Comparator.comparingLong(LogicalQueueRouteData::getLogicalQueueDelta)
                        .thenComparing(LogicalQueueRouteData::getMessageQueue)
                        .thenComparingInt(LogicalQueueRouteData::getStateOrdinal));
                if (idx < 0) {
                    idx = -idx - 1;
                }
                sortedQueueRouteDataList.add(idx, queueRouteData);
            });
        }
        int oldLogicalQueueNum = (int) allLogicalQueueMapByIndex.values().stream().filter(queueRouteDataList -> queueRouteDataList.stream().anyMatch(LogicalQueueRouteData::isWritable)).count();
        if (oldLogicalQueueNum == newLogicalQueueNum) {
            log.info("logical queue num not changed!");
        } else if (oldLogicalQueueNum < newLogicalQueueNum) {
            increaseLogicalQueueNum(defaultMQAdminExt, allLogicalQueueMapByBroker, allLogicalQueueMapByIndex, topicConfigsByBrokerAddr, oldLogicalQueueNum, newLogicalQueueNum);
        } else {
            decreaseLogicalQueueNum(defaultMQAdminExt, allLogicalQueueMapByIndex, oldLogicalQueueNum, newLogicalQueueNum);
        }
    }

    private void increaseLogicalQueueNum(DefaultMQAdminExt defaultMQAdminExt,
        Map<String, LogicalQueuesInfo> allLogicalQueuesInfoMapByBroker,
        NavigableMap<Integer, List<LogicalQueueRouteData>> allLogicalQueueMapByIndex,
        Map<String, TopicConfig> topicConfigsByBrokerAddr, int oldLogicalQueueNum,
        int newLogicalQueueNum) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        int curLogicalQueueNum = oldLogicalQueueNum;
        String topic = topicConfigsByBrokerAddr.values().stream().findAny().map(TopicConfig::getTopicName).get();
        // try use queue not be assigned as logical queue
        for (Map.Entry<String, TopicConfig> e : topicConfigsByBrokerAddr.entrySet()) {
            String brokerAddr = e.getKey();
            TopicConfig topicConfig = e.getValue();
            LogicalQueuesInfo logicalQueuesInfo = allLogicalQueuesInfoMapByBroker.getOrDefault(brokerAddr, new LogicalQueuesInfo());
            ListMultimap<Integer, LogicalQueueRouteData> m = Multimaps.index(logicalQueuesInfo.values().stream().flatMap(Collection::stream).iterator(), LogicalQueueRouteData::getQueueId);
            for (int queueId = 0, writeQueueNums = topicConfig.getWriteQueueNums(); queueId < writeQueueNums; queueId++) {
                if (m.get(queueId).stream().anyMatch(LogicalQueueRouteData::isWritable)) {
                    continue;
                }
                int logicalQueueIdx = curLogicalQueueNum;
                LogicalQueueRouteData queueRouteData;
                try {
                    queueRouteData = defaultMQAdminExt.reuseTopicLogicalQueue(brokerAddr, topic, queueId, logicalQueueIdx, MessageQueueRouteState.Normal);
                } finally {
                    log.info("updateTopicLogicalQueueMapping reuse expired message queue from sealing logical queue brokerAddr={} queueId={} logicalQueueIdx={}", brokerAddr, queueId, logicalQueueIdx);
                }
                curLogicalQueueNum++;
                if (curLogicalQueueNum >= newLogicalQueueNum) {
                    return;
                }
                allLogicalQueueMapByIndex.computeIfAbsent(logicalQueueIdx, integer -> Lists.newArrayListWithExpectedSize(1)).add(queueRouteData);
                logicalQueuesInfo.computeIfAbsent(logicalQueueIdx, integer -> Lists.newArrayListWithExpectedSize(1)).add(queueRouteData);
            }
        }
        // try reuse still sealing logical queue
        for (Map.Entry<Integer, List<LogicalQueueRouteData>> entry : allLogicalQueueMapByIndex.entrySet()) {
            List<LogicalQueueRouteData> queueRouteDataList = entry.getValue();
            if (queueRouteDataList.size() == 0 || queueRouteDataList.stream().anyMatch(LogicalQueueRouteData::isWritable)) {
                continue;
            }
            int logicalQueueIdx = entry.getKey();
            // this is a sealing logical queue
            LogicalQueueRouteData queueRouteData = queueRouteDataList.get(queueRouteDataList.size() - 1);
            String brokerAddr = queueRouteData.getBrokerAddr();
            List<LogicalQueueRouteData> queueRouteDataListByBroker = allLogicalQueuesInfoMapByBroker.get(brokerAddr).get(logicalQueueIdx);
            if (queueRouteData.isExpired()) {
                int queueId = queueRouteData.getQueueId();
                try {
                    queueRouteData = defaultMQAdminExt.reuseTopicLogicalQueue(brokerAddr, topic, queueId, logicalQueueIdx, MessageQueueRouteState.Normal);
                } finally {
                    log.info("updateTopicLogicalQueueMapping reuse expired message queue from sealing logical queue brokerAddr={} queueId={} logicalQueueIdx={}", brokerAddr, queueId, logicalQueueIdx);
                }
                queueRouteDataList.add(queueRouteData);
                queueRouteDataListByBroker.add(queueRouteData);
            } else {
                // create a message queue in last broker
                // not expired message queue can not be reused, since delta value will not be described by one `long`
                int queueId = -1;
                try {
                    queueRouteData = defaultMQAdminExt.createMessageQueueForLogicalQueue(brokerAddr, topic, logicalQueueIdx, MessageQueueRouteState.Normal);
                    queueId = queueRouteData.getQueueId();
                } finally {
                    log.info("updateTopicLogicalQueueMapping create message queue from sealing logical queue brokerAddr={} queueId={} logicalQueueIdx={}", brokerAddr, queueId, logicalQueueIdx);
                }
                queueRouteDataList.add(queueRouteData);
                queueRouteDataListByBroker.add(queueRouteData);
                topicConfigsByBrokerAddr.get(brokerAddr).setWriteQueueNums(queueId + 1);
            }
            curLogicalQueueNum++;
            if (curLogicalQueueNum >= newLogicalQueueNum) {
                return;
            }
        }
        // try broker already with expired message queue
        for (Map.Entry<String, LogicalQueuesInfo> entry : allLogicalQueuesInfoMapByBroker.entrySet()) {
            String brokerAddr = entry.getKey();
            for (Iterator<LogicalQueueRouteData> it = entry.getValue().values().stream().flatMap(Collection::stream)
                .filter(LogicalQueueRouteData::isExpired)
                .sorted(Comparator.comparingInt(LogicalQueueRouteData::getLogicalQueueIndex).thenComparingInt(LogicalQueueRouteData::getQueueId))
                .limit(newLogicalQueueNum - curLogicalQueueNum)
                .iterator(); it.hasNext(); ) {
                LogicalQueueRouteData queueRouteData = it.next();
                try {
                    LogicalQueueRouteData result = defaultMQAdminExt.reuseTopicLogicalQueue(brokerAddr, topic, queueRouteData.getQueueId(), queueRouteData.getLogicalQueueIndex(), MessageQueueRouteState.Normal);
                    // modify in-place
                    queueRouteData.copyFrom(result);
                } finally {
                    log.info("updateTopicLogicalQueueMapping reuse expired message queue from sealing logical queue brokerAddr={} queueId={} logicalQueueIdx={}", brokerAddr, queueRouteData.getQueueId(), queueRouteData.getLogicalQueueIndex());
                }
                allLogicalQueueMapByIndex.get(queueRouteData.getLogicalQueueIndex()).stream()
                    .filter(LogicalQueueRouteData::isExpired)
                    .filter(v -> Objects.equals(brokerAddr, v.getBrokerAddr()) && queueRouteData.getQueueId() == v.getQueueId() && queueRouteData.getLogicalQueueIndex() == v.getLogicalQueueIndex())
                    .forEach(v -> v.copyFrom(queueRouteData));
                curLogicalQueueNum++;
                if (curLogicalQueueNum >= newLogicalQueueNum) {
                    return;
                }
            }
        }

        // try broker with least amount message queue, if amount equal, random select
        for (; curLogicalQueueNum < newLogicalQueueNum; curLogicalQueueNum++) {
            Map.Entry<String, LogicalQueuesInfo> entry = allLogicalQueuesInfoMapByBroker.entrySet().stream().min(Comparator.comparingInt(value -> value.getValue().values().stream().flatMapToInt(l -> IntStream.of(l.size())).sum())).get();
            String brokerAddr = entry.getKey();
            int logicalQueueIdx = curLogicalQueueNum;
            int queueId = -1;
            LogicalQueueRouteData queueRouteData;
            try {
                queueRouteData = defaultMQAdminExt.createMessageQueueForLogicalQueue(brokerAddr, topic, logicalQueueIdx, MessageQueueRouteState.Normal);
                queueId = queueRouteData.getQueueId();
            } finally {
                log.info("updateTopicLogicalQueueMapping create message queue from fresh brokerAddr={} queueId={} logicalQueueIdx={}", brokerAddr, queueId, logicalQueueIdx);
            }
            entry.getValue().put(logicalQueueIdx, Lists.newArrayList(queueRouteData));
        }
    }

    private void decreaseLogicalQueueNum(DefaultMQAdminExt defaultMQAdminExt,
        NavigableMap<Integer, List<LogicalQueueRouteData>> allLogicalQueueMapByIndex,
        int oldLogicalQueueNum,
        int newLogicalQueueNum) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, MQBrokerException, SubCommandException {
        // seal logical queue from greatest index
        Map.Entry<Integer, List<LogicalQueueRouteData>> maxActiveEntry = allLogicalQueueMapByIndex.lastEntry();
        int curLogicalQueueNum = oldLogicalQueueNum;
        while (curLogicalQueueNum > newLogicalQueueNum) {
            boolean anyQueueSealed = false;
            for (LogicalQueueRouteData queueRouteData : maxActiveEntry.getValue()) {
                if (queueRouteData.isWritable()) {
                    anyQueueSealed = true;
                    LogicalQueueRouteData resultQueueRouteData = queueRouteData;
                    try {
                        resultQueueRouteData = defaultMQAdminExt.sealTopicLogicalQueue(queueRouteData.getBrokerAddr(), queueRouteData);
                    } finally {
                        log.info("seal message queue: {}", resultQueueRouteData);
                    }
                }
            }
            if (anyQueueSealed) {
                curLogicalQueueNum--;
            }
            maxActiveEntry = allLogicalQueueMapByIndex.lowerEntry(maxActiveEntry.getKey());
            if (maxActiveEntry == null) {
                throw new SubCommandException(String.format(Locale.ENGLISH, "oldLogicalQueueNum=%d newLogicalQueueNum=%d curLogicalQueueNum=%d but can not find lowerEntry, unexpected situation", oldLogicalQueueNum, newLogicalQueueNum, curLogicalQueueNum));
            }
        }
    }
}
