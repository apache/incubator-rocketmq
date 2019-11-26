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

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiPathMappedFileQueue extends MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final MessageStoreConfig config;

    public MultiPathMappedFileQueue(MessageStoreConfig messageStoreConfig, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        super(messageStoreConfig.getStorePathCommitLog(), mappedFileSize, allocateMappedFileService);
        this.config = messageStoreConfig;
    }

    @Override
    public boolean load() {
        List<File> files = new ArrayList<>();
        Set<String> loadPaths = new HashSet<>();
        List<String> commitLogStorePaths = getCommitLogStorePaths();
        if (commitLogStorePaths != null) {
            loadPaths.addAll(commitLogStorePaths);
        }
        List<String> readOnlyStorePaths = getReadOnlyStorePaths();
        if (readOnlyStorePaths != null) {
            loadPaths.addAll(readOnlyStorePaths);
        }
        if (loadPaths.size() > 0) {
            for (String path : loadPaths) {
                File dir = new File(path);
                File[] ls = dir.listFiles();
                if (ls != null) {
                    Collections.addAll(files, ls);
                }
            }
        }
        return doLoad(files);
    }

    private List<String> getCommitLogStorePaths() {
        return StoreUtil.getCommitLogStorePaths(config.getStorePathCommitLog());
    }

    private List<String> getReadOnlyStorePaths() {
        return StoreUtil.getReadOnlyStorePaths(config.getReadOnlyCommitLogStorePaths());
    }

    @Override
    protected MappedFile tryCreateMappedFile(long createOffset) {
        long fileIdx = createOffset / this.mappedFileSize;
        List<String> pathList = checkDiskSpaceAndReturnPaths();
        if (pathList.size() <= 0) {
            return null;
        }
        String nextFilePath = pathList.get((int) (fileIdx % pathList.size())) + File.separator
            + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = pathList.get((int) ((fileIdx + 1) % pathList.size())) + File.separator
            + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    public List<String> checkDiskSpaceAndReturnPaths() {
        List<String> storePaths = new ArrayList<>();
        List<String> checkPaths = getCommitLogStorePaths();
        String readOnlyPathsStr = config.getReadOnlyCommitLogStorePaths();
        if (checkPaths != null) {
            for (String path : checkPaths) {
                if (readOnlyPathsStr != null && readOnlyPathsStr.contains(path)) {
                    continue;
                }
                try {
                    File file = new File(path);
                    MappedFile.ensureDirOK(file.getPath());
                    if (file.exists() && file.getFreeSpace() > this.mappedFileSize) {
                        storePaths.add(path);
                    }
                } catch (Exception e) {
                    log.error("Failed to load the store path{} ex{}", path, e.getMessage());
                }
            }
        }

        return storePaths;
    }

    @Override
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;
        List<String> commitLogStorePaths = getCommitLogStorePaths();
        if (commitLogStorePaths != null) {
            for (String path : commitLogStorePaths) {
                File file = new File(path);
                if (file.isDirectory()) {
                    file.delete();
                }
            }
        }

        List<String> readOnlyCommitLogStorePaths = getReadOnlyStorePaths();
        if (readOnlyCommitLogStorePaths != null) {
            for (String path : readOnlyCommitLogStorePaths) {
                File file = new File(path);
                if (file.isDirectory()) {
                    file.delete();
                }
            }
        }
    }
}
