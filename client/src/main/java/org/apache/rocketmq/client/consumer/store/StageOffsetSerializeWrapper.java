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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * Wrapper class for stage offset serialization
 */
public class StageOffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<MessageQueue, ConcurrentMap<String/*strategyId*/, ConcurrentMap<String/*groupId*/, AtomicInteger/*offset*/>>> offsetTable =
        new ConcurrentHashMap<>();

    public ConcurrentMap<MessageQueue, ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(
        ConcurrentMap<MessageQueue, ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>>> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
