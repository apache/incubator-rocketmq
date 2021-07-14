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

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Refer to {@link org.apache.rocketmq.client.consumer.store.OffsetStore}, manage the stage consumption progress.
 */
public interface StageOffsetStore {
    /**
     * Load
     */
    void load() throws MQClientException;

    /**
     * Update the stage offset,store it in memory
     */
    void updateStageOffset(final MessageQueue mq, final String strategyId, final String groupId, final int stageOffset,
        final boolean increaseOnly);

    /**
     * Get stage offset from local storage
     *
     * @return The fetched offset
     */
    Map<String, Map<String, Integer>> readStageOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * Persist all offsets,may be in local storage or remote name server
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * Remove stage offset
     */
    void removeStageOffset(MessageQueue mq);

    /**
     * @return The cloned stage offset table of given topic
     */
    Map<MessageQueue, Map<String, Map<String, Integer>>> cloneStageOffsetTable(String topic);

    /**
     * @param mq
     * @param groupId
     * @param stageOffset
     * @param isOneway
     */
    void updateConsumeStageOffsetToBroker(MessageQueue mq, String strategyId, String groupId, int stageOffset,
        boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
}
