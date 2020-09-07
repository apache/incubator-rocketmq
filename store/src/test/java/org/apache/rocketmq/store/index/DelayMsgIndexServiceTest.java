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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DelayMsgIndexServiceTest {
    private final int HASH_SLOT_NUM = 100;
    private final int INDEX_NUM = 400;
    private final String STORE_PATH = "test";
    long baseTime = System.currentTimeMillis() / 1000 * 1000 + 120 * 60 * 1000;

    private DefaultMessageStore messageStore;
    private DelayMsgIndexService delayMsgIndexService;
    private MessageStoreConfig storeConfig;

    @Before
    public void setup() {
        messageStore = mock(DefaultMessageStore.class);
        storeConfig = mock(MessageStoreConfig.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        when(storeConfig.getMaxHashSlotNum()).thenReturn(HASH_SLOT_NUM);
        when(storeConfig.getMaxIndexNum()).thenReturn(INDEX_NUM);
        when(storeConfig.getStorePathRootDir()).thenReturn(STORE_PATH);
        delayMsgIndexService = spy(new DelayMsgIndexService(messageStore));
    }

    @Test
    public void testLoad() throws IOException {

        DelayMsgIndexFile indexFile = new DelayMsgIndexFile(StorePathConfigHelper.getStorePathDelayMsgIndex(storeConfig.getStorePathRootDir()) + File.separator + "123456", HASH_SLOT_NUM, INDEX_NUM, 0, 0);
        for (long i = 0; i < (INDEX_NUM - 1); i++) {
            boolean putResult = indexFile.putKey(Long.toString(baseTime + i * 1000), i);
            assertThat(putResult).isTrue();
        }
        delayMsgIndexService.load();
        ScheduleIndex index = delayMsgIndexService.queryDelayMsgOffset(baseTime + 60 * 1000);
        assertTrue(delayMsgIndexService.isFileExist());
        assertEquals(60, index.getPhyOffsets().get(0).longValue());
        delayMsgIndexService.destroy();
        indexFile.destroy(0);
    }

    @Test
    public void testBuildIndex() {
        assertFalse(delayMsgIndexService.isFileExist());

        Map<String, String> properties = new HashMap<>();
        properties.put(MessageConst.PROPERTY_START_DELIVER_TIME, String.valueOf(baseTime));
        DispatchRequest req = new DispatchRequest("topic", 0, 0, 100, 100, 1575686548, 0, "key", "uniqKey", 0, 0, properties);
        delayMsgIndexService.buildIndex(req);
        assertTrue(delayMsgIndexService.isFileExist());

        properties.put(MessageConst.PROPERTY_START_DELIVER_TIME, String.valueOf(baseTime + 1 * 1000));
        req = new DispatchRequest("topic", 0, 100, 100, 100, 1575686548, 0, "key", "uniqKey", 0, 0, properties);
        delayMsgIndexService.buildIndex(req);
        ScheduleIndex index = delayMsgIndexService.queryDelayMsgOffset(baseTime + 1 * 1000);
        assertEquals(100, index.getPhyOffsets().get(0).longValue());
    }

    @Test
    public void testDeleteExpiredFile()  {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < INDEX_NUM + 1; i++) {
            properties.put(MessageConst.PROPERTY_START_DELIVER_TIME, String.valueOf(baseTime + i * 1000));
            DispatchRequest req = new DispatchRequest("topic", 0, i * 100, 100, 100, 1575686548, 0, "key", "uniqKey", 0, 0, properties);
            delayMsgIndexService.buildIndex(req);
        }
        ScheduleIndex index = delayMsgIndexService.queryDelayMsgOffset(baseTime + 10 * 1000);
        assertTrue(index.getPhyOffsets().size() != 0);

        delayMsgIndexService.deleteExpiredFile(baseTime + INDEX_NUM * 1000);

        index = delayMsgIndexService.queryDelayMsgOffset(baseTime + 10 * 1000);
        assertFalse(index.getPhyOffsets().size() != 0);
        delayMsgIndexService.destroy();

    }

    @After
    public void teardown() throws InterruptedException {
        Thread.sleep(3000);
        File file = new File(STORE_PATH);
        UtilAll.deleteFile(file);
    }
}



