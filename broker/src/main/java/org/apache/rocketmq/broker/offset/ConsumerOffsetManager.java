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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumerOffsetManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable =
        new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);
    private final DataVersion dataVersion = new DataVersion();

    private transient BrokerController brokerController;

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void scanUnsubscribedTopic() {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];

                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic)
                    && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();
        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }
        return result;
    }

    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<String>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }
        return topics;
    }

    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<String>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }
        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null)
                return offset;
        }
        return -1;
    }

    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {
        Map<Integer, Long> queueMinOffset = new HashMap<Integer, Long>();
        Set<String> topicGroups = this.offsetTable.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    if (group.equals(it.next().split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                    }
                }
            }
        }
        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            if (topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }
        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<Integer, Long>(offsets));
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void cleanConsumerOffsetList(final String groupName) {
        for (Entry<String, ConcurrentMap<Integer, Long>> map : this.offsetTable.entrySet()) {
            int indexAtGroup = map.getKey().lastIndexOf(TOPIC_GROUP_SEPARATOR + groupName);
            if (indexAtGroup != -1) {
                // this topicName Contain system auto create example:%RETRY%+Topic+Group
                String topic = map.getKey().substring(0,indexAtGroup);
                String cleanTopicAtGroup = topic + TOPIC_GROUP_SEPARATOR + groupName;
                ConcurrentMap<Integer, Long> result = this.offsetTable.remove(cleanTopicAtGroup);
                if (result != null) {
                    log.info("cleanOffset OK in offsetTable  Topic@subscription: {} ", cleanTopicAtGroup);
                    this.dataVersion.nextVersion();
                    this.persist();
                } else {
                    log.warn("cleanOffset failed in offsetTable Topic@subscription: {} not exist", cleanTopicAtGroup);
                }
            }
        }
    }
}
