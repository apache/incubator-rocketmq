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

package org.apache.rocketmq.broker.domain;

import com.alibaba.fastjson.parser.ParserConfig;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.rocketmq.common.fastjson.GenericMapSuperclassDeserializer;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.LogicalQueuesInfo;
import org.apache.rocketmq.srvutil.ConcurrentHashMapUtil;

import static java.util.Optional.ofNullable;

public class LogicalQueuesInfoInBroker extends LogicalQueuesInfo {
    private final ConcurrentMap<Integer, ConcurrentNavigableMap<Long, LogicalQueueRouteData>> queueId2LogicalQueueMap = Maps.newConcurrentMap();

    public LogicalQueuesInfoInBroker() {
    }

    public LogicalQueuesInfoInBroker(LogicalQueuesInfoInBroker other) {
        this(other, null);
    }

    // deep copy
    public LogicalQueuesInfoInBroker(LogicalQueuesInfoInBroker other, Predicate<LogicalQueueRouteData> predicate) {
        other.readLock().lock();
        try {
            for (Entry<Integer, List<LogicalQueueRouteData>> entry : other.entrySet()) {
                Stream<LogicalQueueRouteData> stream = entry.getValue().stream();
                if (predicate != null) {
                    stream = stream.filter(predicate);
                }
                this.put(entry.getKey(), stream.map(LogicalQueueRouteData::new).collect(Collectors.toList()));
            }
        } finally {
            other.readLock().unlock();
        }
    }

    public void updateQueueRouteDataByQueueId(int queueId, LogicalQueueRouteData queueRouteData) {
        if (queueRouteData == null) {
            return;
        }
        ConcurrentHashMapUtil.computeIfAbsent(queueId2LogicalQueueMap, queueId, k -> new ConcurrentSkipListMap<>()).put(queueRouteData.getOffsetDelta(), queueRouteData);
    }

    /**
     * find logical queue route data for message queues owned by this broker
     */
    public LogicalQueueRouteData queryQueueRouteDataByQueueId(int queueId, long offset) {
        ConcurrentNavigableMap<Long, LogicalQueueRouteData> m = this.queueId2LogicalQueueMap.get(queueId);
        if (m == null || m.isEmpty()) {
            return null;
        }
        Entry<Long, LogicalQueueRouteData> entry = m.floorEntry(offset);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public void deleteQueueRouteData(LogicalQueueRouteData logicalQueueRouteData) {
        ConcurrentNavigableMap<Long, LogicalQueueRouteData> m = this.queueId2LogicalQueueMap.get(logicalQueueRouteData.getQueueId());
        if (m != null) {
            m.remove(logicalQueueRouteData.getOffsetDelta(), logicalQueueRouteData);
        }
    }

    public LogicalQueueRouteData nextAvailableLogicalRouteData(LogicalQueueRouteData queueRouteData,
        Predicate<LogicalQueueRouteData> predicate) {
        this.readLock().lock();
        try {
            List<LogicalQueueRouteData> queueRouteDataList = ofNullable(this.get(queueRouteData.getLogicalQueueIndex())).orElse(Collections.emptyList());
            int idx = Collections.binarySearch(queueRouteDataList, queueRouteData);
            if (idx >= 0) {
                for (int i = idx + 1, size = queueRouteDataList.size(); i < size; i++) {
                    LogicalQueueRouteData tmp = queueRouteDataList.get(i);
                    if (predicate.test(tmp)) {
                        return tmp;
                    }
                }
            }
        } finally {
            this.readLock().unlock();
        }
        return null;
    }

    static {
        // workaround https://github.com/alibaba/fastjson/issues/3730
        ParserConfig.getGlobalInstance().putDeserializer(LogicalQueuesInfoInBroker.class, GenericMapSuperclassDeserializer.INSTANCE);
    }
}
