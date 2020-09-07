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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DelayMsgIndexFileTest {
    private final int HASH_SLOT_NUM = 100;
    private final int INDEX_NUM = 400;

    @Test
    public void testPutKey() throws Exception {
        DelayMsgIndexFile indexFile = new DelayMsgIndexFile("100", HASH_SLOT_NUM, INDEX_NUM, 0, 0);
        long baseTime= System.currentTimeMillis()/1000*1000+120*60*1000;
        for (long i = 0; i < (INDEX_NUM - 1); i++) {
            boolean putResult = indexFile.putKey(Long.toString(baseTime+i*1000), i);
            assertThat(putResult).isTrue();
        }

        // put over index file capacity.
        boolean putResult = indexFile.putKey(Long.toString(baseTime+400*1000), 400);
        assertThat(putResult).isFalse();
        indexFile.destroy(0);
        File file = new File("100");
        UtilAll.deleteFile(file);
    }

    @Test
    public void testSelectPhyOffset() throws Exception {
        DelayMsgIndexFile indexFile = new DelayMsgIndexFile("200", HASH_SLOT_NUM, INDEX_NUM, 0, 0);
        long baseTime= System.currentTimeMillis()/1000*1000+120*60*1000;
        for (long i = 0; i < (INDEX_NUM - 1); i++) {
            boolean putResult = indexFile.putKey(Long.toString(baseTime+i*1000),60);
            assertThat(putResult).isTrue();
        }

        final List<Long> phyOffsets = new ArrayList<>();
        indexFile.selectPhyOffset(phyOffsets, Long.toString(baseTime+60*1000), true);
        assertThat(phyOffsets).isNotEmpty();
        assertThat(phyOffsets.size()).isEqualTo(1);
        indexFile.destroy(0);
        File file = new File("200");
        UtilAll.deleteFile(file);
    }
}
