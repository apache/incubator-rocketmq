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
package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumerStageOffsetManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    private ConcurrentMap<String/*topic@group*/, ConcurrentMap<Integer/*queueId*/, ConcurrentMap<String/*strategyId*/, ConcurrentMap<String/*groupId*/, Integer/*offset*/>>>> offsetTable =
        new ConcurrentHashMap<>(256);

    private transient BrokerController brokerController;

    public ConsumerStageOffsetManager() {
    }

    public ConsumerStageOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void commitStageOffset(final String clientHost, final String group, final String topic, final int queueId,
        final String strategyId, String groupId, final int offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitStageOffset(clientHost, key, queueId, strategyId, groupId, offset);
    }

    private void commitStageOffset(final String clientHost, final String key, final int queueId,
        final String strategyId, String groupId, final int offset) {
        ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>> map = this.offsetTable.putIfAbsent(key, new ConcurrentHashMap<>(32));
        if (null == map) {
            map = this.offsetTable.get(key);
            ConcurrentMap<String, ConcurrentMap<String, Integer>> strategies = new ConcurrentHashMap<>();
            ConcurrentMap<String, Integer> groups = new ConcurrentHashMap<>();
            groups.put(groupId, offset);
            strategies.put(strategyId, groups);
            map.put(queueId, strategies);
        } else {
            ConcurrentMap<String, ConcurrentMap<String, Integer>> strategies = map.putIfAbsent(queueId, new ConcurrentHashMap<>(32));
            if (null == strategies) {
                strategies = map.get(queueId);
            }
            ConcurrentMap<String, Integer> groups = strategies.putIfAbsent(strategyId, new ConcurrentHashMap<>(32));
            if (null == groups) {
                groups = strategies.get(strategyId);
            }
            Integer storeOffset = groups.put(groupId, offset);
            map.put(queueId, strategies);
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer stage offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    public Map<String, Map<String, Integer>> queryStageOffset(final String group, final String topic,
        final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>> map = this.offsetTable.get(key);
        if (null != map) {
            Map<String, ConcurrentMap<String, Integer>> strategies = map.get(queueId);
            if (strategies != null) {
                Map<String, Map<String, Integer>> result = new HashMap<>(strategies.size());
                strategies.forEach((strategy, groups) -> {
                    result.put(strategy, new HashMap<>(groups));
                });
                return result;
            }
        }

        return new HashMap<>();
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerStageOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerStageOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerStageOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(
        ConcurrentMap<String, ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>> queryStageOffset(
        final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    public void cloneStageOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<>(offsets));
        }
    }

    public void removeStageOffset(final String group) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConcurrentMap<String, ConcurrentMap<String, Integer>>>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    log.warn("clean group offset {}", topicAtGroup);
                }
            }
        }

    }

}
