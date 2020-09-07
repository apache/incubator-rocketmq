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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.Optional;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DelayMsgIndexService extends Observable {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    private final int hashSlotNum;
    private final int indexNum;
    private final String storePath;
    private final ArrayList<DelayMsgIndexFile> indexFileList = new ArrayList<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private boolean isFileExist = false;

    public DelayMsgIndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath = StorePathConfigHelper.getStorePathDelayMsgIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            isFileExist = true;
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    DelayMsgIndexFile f = new DelayMsgIndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    LOGGER.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    LOGGER.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    LOGGER.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public void deleteExpiredFile(long time) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            files = this.indexFileList.toArray();

        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<DelayMsgIndexFile> fileList = new ArrayList<>();
            for (int i = 0; i < (files.length - 1); i++) {
                DelayMsgIndexFile f = (DelayMsgIndexFile) files[i];
                if (f.getEndTimestamp() < time) {
                    fileList.add(f);
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<DelayMsgIndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (DelayMsgIndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        LOGGER.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (DelayMsgIndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public ScheduleIndex queryDelayMsgOffset(long startDeliverTime) {
        List<Long> phyOffsets = new ArrayList<>();
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    DelayMsgIndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (f.isTimeMatched(startDeliverTime)) {
                        f.selectPhyOffset(phyOffsets, String.valueOf(startDeliverTime), lastFile);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return new ScheduleIndex(phyOffsets, startDeliverTime);
    }

    public void buildIndex(DispatchRequest req) {
        String startDeliverTime = Optional.ofNullable(req.getPropertiesMap()).orElse(new HashMap<>(0)).get(MessageConst.PROPERTY_START_DELIVER_TIME);
        if (startDeliverTime == null) {
            return;
        }

        DelayMsgIndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            isFileExist = true;
            long endPhyOffset = indexFile.getEndPhyOffset();
            if (req.getCommitLogOffset() <= endPhyOffset && endPhyOffset != 0) {
                return;
            }


            indexFile = putKey(indexFile, req, startDeliverTime);
            if (indexFile == null) {
                LOGGER.error("putKey error commitlog {} ", req.getCommitLogOffset());
                return;
            }
            setChanged();
            notifyObservers(new ScheduleIndex(Collections.singletonList(req.getCommitLogOffset()), Long.parseLong(startDeliverTime)));

        } else {
            LOGGER.error("build index error, stop building index");
        }
    }

    private DelayMsgIndexFile putKey(DelayMsgIndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset()); !ok; ) {
            LOGGER.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     *
     * @return {@link DelayMsgIndexFile} or null on failure.
     */
    public DelayMsgIndexFile retryGetAndCreateIndexFile() {
        DelayMsgIndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile) {
                break;
            }

            try {
                LOGGER.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            LOGGER.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    public DelayMsgIndexFile getAndCreateLastIndexFile() {
        DelayMsgIndexFile indexFile = null;
        DelayMsgIndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                DelayMsgIndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        if (indexFile == null) {
            try {
                String fileName =
                        this.storePath + File.separator
                                + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                indexFile =
                        new DelayMsgIndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                                lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                LOGGER.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {
                final DelayMsgIndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(() -> DelayMsgIndexService.this.flush(flushThisFile), "FlushDelayMsgIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final DelayMsgIndexFile f) {
        if (null == f) {
            return;
        }
        f.flush();


    }

    public void start() {

    }

    public void shutdown() {

    }

    public boolean isFileExist() {
        return isFileExist;
    }
}
