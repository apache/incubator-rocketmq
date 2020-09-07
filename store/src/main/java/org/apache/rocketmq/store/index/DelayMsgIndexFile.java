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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

public class DelayMsgIndexFile {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);
    private static final int HASH_SLOT_SIZE = 4;
    private static final int INDEX_SIZE = 20;
    private static final int INVALID_INDEX = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public DelayMsgIndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                             final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * HASH_SLOT_SIZE) + (indexNum * INDEX_SIZE);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            LOGGER.info("Flush index file elapsed {}  time(ms) ", System.currentTimeMillis() - beginTime);
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * write index info into indexfile
     *
     * @param key       timestamp
     * @param phyOffset physical offset in commitlog
     * @return putKey result, true or false
     */
    public boolean putKey(final String key, final long phyOffset) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            long scheduleTime = Long.parseLong(key) / 1000 * 1000;
            int keyHash = indexKeyHashMethod(String.valueOf(scheduleTime));
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= INVALID_INDEX || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = INVALID_INDEX;
                }

                long timeDiff = 0;

                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * HASH_SLOT_SIZE
                                + this.indexHeader.getIndexCount() * INDEX_SIZE;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(scheduleTime);
                    this.indexHeader.setEndTimestamp(scheduleTime);
                }


                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                if (scheduleTime < this.indexHeader.getBeginTimestamp()) {
                    this.indexHeader.setBeginTimestamp(scheduleTime);
                }
                if (scheduleTime > this.indexHeader.getEndTimestamp()) {
                    this.indexHeader.setEndTimestamp(scheduleTime);
                }

                return true;
            } catch (Exception e) {
                LOGGER.error("PutKey exception, Key: " + scheduleTime + " KeyHashCode: " + String.valueOf(scheduleTime).hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        LOGGER.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            LOGGER.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long startDeliverTime) {
        return startDeliverTime >= this.indexHeader.getBeginTimestamp() && startDeliverTime <= this.indexHeader.getEndTimestamp();

    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, boolean lock) {
        if (this.mappedFile.hold()) {

            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= INVALID_INDEX || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {

                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * HASH_SLOT_SIZE
                                        + nextIndexToRead * INDEX_SIZE;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }


                        if (keyHash == keyHashRead) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= INVALID_INDEX
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("SelectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        LOGGER.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}

