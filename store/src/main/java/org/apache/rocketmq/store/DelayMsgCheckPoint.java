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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Observable;
import java.util.Observer;

public class DelayMsgCheckPoint implements Observer {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private volatile long delayMsgTimestamp = 0;

    public DelayMsgCheckPoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            LOGGER.info("DelayMsgCheckPoint file exists, {}", scpPath);
            this.delayMsgTimestamp = this.mappedByteBuffer.getLong(0) / 1000 * 1000;

            LOGGER.info("DelayMsgCheckPoint file delayMsgTimestamp {}, {}", this.delayMsgTimestamp,
                    UtilAll.timeMillisToHumanString2(this.delayMsgTimestamp));
        } else {
            LOGGER.info("DelayMsgCheckPoint file not exists, {}", scpPath);
        }
    }

    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            LOGGER.error("Failed to properly close the channel, {}", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.delayMsgTimestamp);
        this.mappedByteBuffer.force();
    }

    public long getDelayMsgTimestamp() {
        return delayMsgTimestamp;
    }

    public void setDelayMsgTimestamp(long delayMsgTimestamp) {
        this.delayMsgTimestamp = delayMsgTimestamp;
    }

    @Override
    public void update(Observable o, Object arg) {
        this.setDelayMsgTimestamp((Long) arg);
        this.flush();
    }
}
