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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Local storage implementation
 */
public class LocalFileStageOffsetStore extends AbstractStageOffsetStore {
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        System.getProperty("user.home") + File.separator + ".rocketmq_stage_offsets");
    private final String storePath;

    public LocalFileStageOffsetStore(MQClientInstance mqClientFactory, String groupName) {
        super(mqClientFactory, groupName);
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            this.mqClientFactory.getClientId() + File.separator +
            this.groupName + File.separator +
            "stageOffsets.json";
    }

    @Override
    public void load() throws MQClientException {
        StageOffsetSerializeWrapper stageOffsetSerializeWrapper = this.readLocalOffset();
        if (stageOffsetSerializeWrapper != null && stageOffsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(stageOffsetSerializeWrapper.getOffsetTable());
            stageOffsetSerializeWrapper.getOffsetTable().forEach((mq, strategies) -> {
                strategies.forEach((strategyId, groups) -> {
                    groups.forEach((groupId, offset) -> {
                        log.info("load consumer's offset, {} {} {} {} {}",
                            this.groupName,
                            mq,
                            strategyId,
                            groupId,
                            offset.get());
                    });
                });
            });
        }
    }

    @Override
    public Map<String, Map<String, Integer>> readStageOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> map = this.offsetTable.get(mq);
                    if (map != null) {
                        return convert(map);
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return new HashMap<>();
                    }
                }
                case READ_FROM_STORE: {
                    StageOffsetSerializeWrapper stageOffsetSerializeWrapper;
                    try {
                        stageOffsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return new HashMap<>();
                    }
                    if (stageOffsetSerializeWrapper != null && stageOffsetSerializeWrapper.getOffsetTable() != null) {
                        ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> map = stageOffsetSerializeWrapper.getOffsetTable().get(mq);
                        if (map != null) {
                            map.forEach((strategy, groups) -> {
                                groups.forEach((group, offset) -> {
                                    this.updateStageOffset(mq, strategy, group, offset.get(), false);
                                });
                            });
                            return convert(map);
                        }
                    }
                }
                default:
                    break;
            }
        }

        return new HashMap<>();
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty()) {
            return;
        }

        StageOffsetSerializeWrapper stageOffsetSerializeWrapper = new StageOffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>>> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> offset = entry.getValue();
                stageOffsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = stageOffsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    private StageOffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            StageOffsetSerializeWrapper stageOffsetSerializeWrapper = null;
            try {
                stageOffsetSerializeWrapper =
                    StageOffsetSerializeWrapper.fromJson(content, StageOffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return stageOffsetSerializeWrapper;
        }
    }

    private StageOffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            StageOffsetSerializeWrapper stageOffsetSerializeWrapper = null;
            try {
                stageOffsetSerializeWrapper =
                    StageOffsetSerializeWrapper.fromJson(content, StageOffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                    + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                    e);
            }
            return stageOffsetSerializeWrapper;
        }

        return null;
    }
}
