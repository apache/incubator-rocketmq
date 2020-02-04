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

package org.apache.rocketmq.broker.plugin;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public abstract class AbstractPluginMessageStore implements MessageStore {
    protected MessageStore next = null;
    protected MessageStorePluginContext context;

    public AbstractPluginMessageStore(MessageStorePluginContext context, MessageStore next) {
        this.next = next;
        this.context = context;
    }

    @Override
    public long getEarliestMessageTime() {
        return next.getEarliestMessageTime();
    }

    @Override
    public long lockTimeMills() {
        return next.lockTimeMills();
    }

    @Override
    public boolean isOSPageCacheBusy() {
        return next.isOSPageCacheBusy();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return next.isTransientStorePoolDeficient();
    }

    @Override
    public boolean load() {
        return next.load();
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void shutdown() {
        next.shutdown();
    }

    @Override
    public void destroy() {
        next.destroy();
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return next.putMessage(msg);
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset,
        int maxMsgNums, final MessageFilter messageFilter) {
        return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        return next.getMaxOffsetInQueue(topic, queueId);
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        return next.getMinOffsetInQueue(topic, queueId);
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        return next.getCommitLogOffsetInQueue(topic, queueId, consumeQueueOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return next.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return next.lookMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return next.selectOneMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return next.selectOneMessageByOffset(commitLogOffset, msgSize);
    }

    @Override
    public String getRunningDataInfo() {
        return next.getRunningDataInfo();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        return next.getRuntimeInfo();
    }

    @Override
    public long getMaxPhyOffset() {
        return next.getMaxPhyOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return next.getMinPhyOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return next.getEarliestMessageTime(topic, queueId);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        return next.getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset);
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        return next.getMessageTotalInQueue(topic, queueId);
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(long offset) {
        return next.getCommitLogData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        return next.appendToCommitLog(startOffset, data);
    }

    @Override
    public void executeDeleteFilesManually() {
        next.executeDeleteFilesManually();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin,
        long end) {
        return next.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        next.updateHaMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return next.slaveFallBehindMuch();
    }

    @Override
    public long now() {
        return next.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        return next.cleanUnusedTopic(topics);
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        next.cleanExpiredConsumerQueue();
    }

    @Override
    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return next.checkInDiskByConsumeOffset(topic, queueId, consumeOffset);
    }

    @Override
    public long dispatchBehindBytes() {
        return next.dispatchBehindBytes();
    }

    @Override
    public long flush() {
        return next.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return next.resetWriteOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return next.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        next.setConfirmOffset(phyOffset);
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return next.getDispatcherList();
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        return next.getConsumeQueue(topic, queueId);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return next.getBrokerStatsManager();
    };
}
