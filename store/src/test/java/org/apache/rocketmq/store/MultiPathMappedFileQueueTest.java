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

package org.apache.rocketmq.store;

import java.util.List;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiPathMappedFileQueueTest {

    @Test
    public void testGetLastMappedFile() {
        final byte[] fixedMsg = new byte[1024];

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/;target/unit_test_store/b/;target/unit_test_store/c/");
        MultiPathMappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null);
        List<String> commitLogStorePaths = StoreUtil.getCommitLogStorePaths(config.getStorePathCommitLog());
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
            assertThat(mappedFile).isNotNull();
            assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
            int idx = i % commitLogStorePaths.size();
            assertThat(mappedFile.getFileName().startsWith(commitLogStorePaths.get(idx))).isTrue();
        }
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testLoadReadOnlyMappedFiles() {
        {
            //create old mapped files
            final byte[] fixedMsg = new byte[1024];
            MessageStoreConfig config = new MessageStoreConfig();
            config.setStorePathCommitLog("target/unit_test_store/a/;target/unit_test_store/b/;target/unit_test_store/c/");
            MultiPathMappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null);
            List<String> commitLogStorePaths = StoreUtil.getCommitLogStorePaths(config.getStorePathCommitLog());
            for (int i = 0; i < 1024; i++) {
                MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
                assertThat(mappedFile).isNotNull();
                assertThat(mappedFile.appendMessage(fixedMsg)).isTrue();
                int idx = i % commitLogStorePaths.size();
                assertThat(mappedFile.getFileName().startsWith(commitLogStorePaths.get(idx))).isTrue();
            }
            mappedFileQueue.shutdown(1000);
        }

        // test load and readonly
        {
            MessageStoreConfig config = new MessageStoreConfig();
            config.setStorePathCommitLog("target/unit_test_store/b/");
            config.setReadOnlyCommitLogStorePaths("target/unit_test_store/a/;target/unit_test_store/c/");
            MultiPathMappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null);
            mappedFileQueue.load();
            assertThat(mappedFileQueue.mappedFiles.size()).isEqualTo(1024);
            mappedFileQueue.shutdown(1000);
            mappedFileQueue.destroy();
        }

    }

    @Test
    public void testUpdatePathsOnline() {
        final byte[] fixedMsg = new byte[1024];

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/;target/unit_test_store/b/;target/unit_test_store/c/");
        MultiPathMappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null);
        assertThat(mappedFileQueue.checkDiskSpaceAndReturnPaths().size()).isEqualTo(3);

        config.setStorePathCommitLog("target/unit_test_store/a/;target/unit_test_store/b/");
        assertThat(mappedFileQueue.checkDiskSpaceAndReturnPaths().size()).isEqualTo(2);

        config.setReadOnlyCommitLogStorePaths("target/unit_test_store/b/");
        assertThat(mappedFileQueue.checkDiskSpaceAndReturnPaths().size()).isEqualTo(1);

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }
}