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
package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

public abstract class AbstractStageOffsetStore implements StageOffsetStore {

    protected final static InternalLogger log = ClientLogger.getLog();

    protected final MQClientInstance mqClientFactory;

    protected final String groupName;

    protected ConcurrentMap<MessageQueue, ConcurrentMap<String/*strategyId*/, ConcurrentMap<String/*groupId*/, AtomicInteger/*offset*/>>> offsetTable =
        new ConcurrentHashMap<>(64);

    public AbstractStageOffsetStore(MQClientInstance mqClientFactory, String groupName) {
        this.mqClientFactory = mqClientFactory;
        this.groupName = groupName;
    }

    @Override
    public void load() throws MQClientException {

    }

    @Override
    public void updateStageOffset(MessageQueue mq, String strategyId, String groupId, int stageOffset,
        boolean increaseOnly) {
        if (mq != null) {
            //avoid null string
            strategyId = String.valueOf(strategyId);
            groupId = String.valueOf(groupId);

            ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> groupByStrategy = this.offsetTable.putIfAbsent(mq, new ConcurrentHashMap<>());
            if (null == groupByStrategy) {
                groupByStrategy = this.offsetTable.get(mq);
            }
            ConcurrentMap<String, AtomicInteger> groups = groupByStrategy.putIfAbsent(strategyId, new ConcurrentHashMap<>());
            if (null == groups) {
                groups = groupByStrategy.get(strategyId);
            }
            AtomicInteger offsetOld = groups.putIfAbsent(groupId, new AtomicInteger(stageOffset));
            if (null == offsetOld) {
                offsetOld = groups.get(groupId);
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, stageOffset);
                } else {
                    offsetOld.set(stageOffset);
                }
            }
        }
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {

    }

    @Override
    public void persist(MessageQueue mq) {

    }

    @Override
    public void removeStageOffset(MessageQueue mq) {

    }

    @Override
    public Map<MessageQueue, Map<String, Map<String, Integer>>> cloneStageOffsetTable(String topic) {
        Map<MessageQueue, Map<String, Map<String, Integer>>> cloneOffsetTable = new HashMap<>(this.offsetTable.size());
        for (Map.Entry<MessageQueue, ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>>> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, convert(entry.getValue()));
        }
        return cloneOffsetTable;
    }

    @Override
    public void updateConsumeStageOffsetToBroker(MessageQueue mq, String strategyId, String groupId,
        int stageOffset,
        boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    protected final Map<String, Map<String, Integer>> convert(
        ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> original) {
        Map<String, Map<String, Integer>> result = new HashMap<>(original.size());
        original.forEach((strategy, groups) -> {
            Map<String, Integer> temp = new HashMap<>();
            groups.forEach((group, offset) -> {
                temp.put(group, offset.get());
            });
            result.put(strategy, temp);
        });
        return result;
    }
}
