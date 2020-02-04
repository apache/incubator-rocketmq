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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

import static org.apache.rocketmq.common.message.MessageDecoder.CHARSET_UTF8;
import static org.apache.rocketmq.store.ConsumeQueue.CQ_STORE_UNIT_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test case for DefaultMessageStore.CleanCommitLogService and DefaultMessageStore.CleanConsumeQueueService
 */
public class DefaultMessageStoreCleanFilesTest {
    private DefaultMessageStore messageStore;
    private DefaultMessageStore.CleanCommitLogService cleanCommitLogService;
    private DefaultMessageStore.CleanConsumeQueueService cleanConsumeQueueService;

    private SocketAddress bornHost;
    private SocketAddress storeHost;

    private String topic = "test";
    private int queueId = 0;
    private int fileCountCommitLog = 55;
    // exactly one message per CommitLog file.
    private int msgCount = fileCountCommitLog;
    private int mappedFileSize = 128;
    private int fileReservedTime = 1;

    @Before
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    @Test
    public void testDeleteExpiredFilesByTimeUp() throws Exception {
        String deleteWhen = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + "";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 99;
        // used to ensure that automatic file deletion is not triggered
        double diskSpaceCleanForciblyRatio = 0.999D;
        initMessageStore(deleteWhen, diskMaxUsedSpaceRatio, diskSpaceCleanForciblyRatio);

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        int expireFileCount = 15;
        expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = expireFileCount; a <= (int) Math.ceil((double) expireFileCount / 10); a++, fileCount -= 10) {
            cleanCommitLogService.run();
            cleanConsumeQueueService.run();

            int expectDeletedCount = fileCount >= 10 ? a * 10 : ((a - 1) * 10 + fileCount);
            assertEquals(fileCountCommitLog - expectDeletedCount, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) expectDeletedCount / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    @Test
    public void testDeleteExpiredFilesBySpaceFull() throws Exception {
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio.
        int diskMaxUsedSpaceRatio = 1;
        // used to ensure that automatic file deletion is not triggered
        double diskSpaceCleanForciblyRatio = 0.999D;
        initMessageStore(deleteWhen, diskMaxUsedSpaceRatio, diskSpaceCleanForciblyRatio);

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        int expireFileCount = 15;
        expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = expireFileCount; a <= (int) Math.ceil((double) expireFileCount / 10); a++, fileCount -= 10) {
            cleanCommitLogService.run();
            cleanConsumeQueueService.run();

            int expectDeletedCount = fileCount >= 10 ? a * 10 : ((a - 1) * 10 + fileCount);
            assertEquals(fileCountCommitLog - expectDeletedCount, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) expectDeletedCount / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    @Test
    public void testDeleteFilesImmediatelyBySpaceFull() throws Exception {
        String deleteWhen = "04";
        // the min value of diskMaxUsedSpaceRatio.
        int diskMaxUsedSpaceRatio = 1;
        // make sure to trigger the automatic file deletion feature
        double diskSpaceCleanForciblyRatio = 0.01D;
        initMessageStore(deleteWhen, diskMaxUsedSpaceRatio, diskSpaceCleanForciblyRatio);

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        // In this case, there is no need to expire the files.
        // int expireFileCount = 15;
        // expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = fileCountCommitLog;
             a <= (int) Math.ceil((double) fileCountCommitLog / 10) && fileCount >= 10;
             a++, fileCount -= 10) {
            cleanCommitLogService.run();
            cleanConsumeQueueService.run();

            assertEquals(fileCountCommitLog - 10 * a, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) (a * 10) / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    @Test
    public void testDeleteExpiredFilesManually() throws Exception {
        String deleteWhen = "04";
        // the max value of diskMaxUsedSpaceRatio
        int diskMaxUsedSpaceRatio = 99;
        // used to ensure that automatic file deletion is not triggered
        double diskSpaceCleanForciblyRatio = 0.999D;
        initMessageStore(deleteWhen, diskMaxUsedSpaceRatio, diskSpaceCleanForciblyRatio);

        messageStore.executeDeleteFilesManually();

        // build and put 55 messages, exactly one message per CommitLog file.
        buildAndPutMessagesToMessageStore(msgCount);

        // undo comment out the code below, if want to debug this case rather than just run it.
        // Thread.sleep(1000 * 60 + 100);

        MappedFileQueue commitLogQueue = getMappedFileQueueCommitLog();
        assertEquals(fileCountCommitLog, commitLogQueue.getMappedFiles().size());

        int fileCountConsumeQueue = getFileCountConsumeQueue();
        MappedFileQueue consumeQueue = getMappedFileQueueConsumeQueue();
        assertEquals(fileCountConsumeQueue, consumeQueue.getMappedFiles().size());

        int expireFileCount = 15;
        expireFiles(commitLogQueue, expireFileCount);

        // magic code 10 reference to MappedFileQueue#DELETE_FILES_BATCH_MAX
        for (int a = 1, fileCount = expireFileCount; a <= (int) Math.ceil((double) expireFileCount / 10); a++, fileCount -= 10) {
            cleanCommitLogService.run();
            cleanConsumeQueueService.run();

            int expectDeletedCount = fileCount >= 10 ? a * 10 : ((a - 1) * 10 + fileCount);
            assertEquals(fileCountCommitLog - expectDeletedCount, commitLogQueue.getMappedFiles().size());

            int msgCountPerFile = getMsgCountPerConsumeQueueMappedFile();
            int expectDeleteCountConsumeQueue = (int) Math.floor((double) expectDeletedCount / msgCountPerFile);
            assertEquals(fileCountConsumeQueue - expectDeleteCountConsumeQueue, consumeQueue.getMappedFiles().size());
        }
    }

    private DefaultMessageStore.CleanCommitLogService getCleanCommitLogService(double diskSpaceCleanForciblyRatio)
            throws Exception {
        Field serviceField = messageStore.getClass().getDeclaredField("cleanCommitLogService");
        serviceField.setAccessible(true);
        DefaultMessageStore.CleanCommitLogService cleanCommitLogService =
                (DefaultMessageStore.CleanCommitLogService) serviceField.get(messageStore);
        serviceField.setAccessible(false);

        Field warningLevelRatioField = cleanCommitLogService.getClass().getDeclaredField("diskSpaceWarningLevelRatio");
        warningLevelRatioField.setAccessible(true);
        warningLevelRatioField.set(cleanCommitLogService, diskSpaceCleanForciblyRatio);
        warningLevelRatioField.setAccessible(false);

        Field cleanForciblyRatioField = cleanCommitLogService.getClass().getDeclaredField("diskSpaceCleanForciblyRatio");
        cleanForciblyRatioField.setAccessible(true);
        cleanForciblyRatioField.set(cleanCommitLogService, diskSpaceCleanForciblyRatio);
        cleanForciblyRatioField.setAccessible(false);
        return cleanCommitLogService;
    }

    private DefaultMessageStore.CleanConsumeQueueService getCleanConsumeQueueService()
            throws Exception {
        Field serviceField = messageStore.getClass().getDeclaredField("cleanConsumeQueueService");
        serviceField.setAccessible(true);
        DefaultMessageStore.CleanConsumeQueueService cleanConsumeQueueService =
                (DefaultMessageStore.CleanConsumeQueueService) serviceField.get(messageStore);
        serviceField.setAccessible(false);
        return cleanConsumeQueueService;
    }

    private MappedFileQueue getMappedFileQueueConsumeQueue()
            throws Exception {
        ConsumeQueue consumeQueue = messageStore.getConsumeQueueTable().get(topic).get(queueId);
        Field queueField = consumeQueue.getClass().getDeclaredField("mappedFileQueue");
        queueField.setAccessible(true);
        MappedFileQueue fileQueue = (MappedFileQueue) queueField.get(consumeQueue);
        queueField.setAccessible(false);
        return fileQueue;
    }

    private MappedFileQueue getMappedFileQueueCommitLog() throws Exception {
        CommitLog commitLog = messageStore.getCommitLog();
        Field queueField = commitLog.getClass().getDeclaredField("mappedFileQueue");
        queueField.setAccessible(true);
        MappedFileQueue fileQueue = (MappedFileQueue) queueField.get(commitLog);
        queueField.setAccessible(false);
        return fileQueue;
    }

    private int getFileCountConsumeQueue() {
        int countPerFile = getMsgCountPerConsumeQueueMappedFile();
        double fileCount = (double) msgCount / countPerFile;
        return (int) Math.ceil(fileCount);
    }

    private int getMsgCountPerConsumeQueueMappedFile() {
        int size = messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueue();
        return size / CQ_STORE_UNIT_SIZE;// 7 in this case
    }

    private void buildAndPutMessagesToMessageStore(int msgCount) throws Exception {
        int msgLen = topic.getBytes(CHARSET_UTF8).length + 91;
        int commitLogEndFileMinBlankLength = 4 + 4;
        int singleMsgBodyLen = mappedFileSize - msgLen - commitLogEndFileMinBlankLength;

        for (int i = 0; i < msgCount; i++) {
            MessageExtBrokerInner msg = new MessageExtBrokerInner();
            msg.setTopic(topic);
            msg.setBody(new byte[singleMsgBodyLen]);
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            msg.setQueueId(queueId);
            msg.setSysFlag(0);
            msg.setBornTimestamp(System.currentTimeMillis());
            msg.setStoreHost(storeHost);
            msg.setBornHost(bornHost);
            PutMessageResult result = messageStore.putMessage(msg);
            assertTrue(result != null && result.isOk());
        }

        StoreTestUtil.waitCommitLogReput(messageStore);
        StoreTestUtil.flushConsumeQueue(messageStore);
        StoreTestUtil.flushConsumeIndex(messageStore);
    }

    private void expireFiles(MappedFileQueue commitLogQueue, int expireCount) {
        for (int i = 0; i < commitLogQueue.getMappedFiles().size(); i++) {
            MappedFile mappedFile = commitLogQueue.getMappedFiles().get(i);
            int reservedTime = fileReservedTime * 60 * 60 * 1000;
            if (i < expireCount) {
                boolean modified = mappedFile.getFile().setLastModified(System.currentTimeMillis() - reservedTime * 2);
                assertTrue(modified);
            }
        }
    }

    private void initMessageStore(String deleteWhen, int diskMaxUsedSpaceRatio, double diskSpaceCleanForciblyRatio) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfigForTest();
        messageStoreConfig.setMappedFileSizeCommitLog(mappedFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(mappedFileSize);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);

        // Invalidate DefaultMessageStore`s scheduled task of cleaning expired files.
        // work with the code 'Thread.sleep(1000 * 60 + 100)' behind.
        messageStoreConfig.setCleanResourceInterval(Integer.MAX_VALUE);

        messageStoreConfig.setFileReservedTime(fileReservedTime);
        messageStoreConfig.setDeleteWhen(deleteWhen);
        messageStoreConfig.setDiskMaxUsedSpaceRatio(diskMaxUsedSpaceRatio);

        String storePathRootDir = System.getProperty("user.home") + File.separator
                + "DefaultMessageStoreCleanFilesTest-" + UUID.randomUUID();
        String storePathCommitLog = storePathRootDir + File.separator + "commitlog";
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        messageStoreConfig.setStorePathCommitLog(storePathCommitLog);

        messageStore = new DefaultMessageStore(messageStoreConfig,
                new BrokerStatsManager("test"), new MyMessageArrivingListener(), new BrokerConfig());

        cleanCommitLogService = getCleanCommitLogService(diskSpaceCleanForciblyRatio);
        cleanConsumeQueueService = getCleanConsumeQueueService();

        assertTrue(messageStore.load());
        messageStore.start();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private class MessageStoreConfigForTest extends MessageStoreConfig {
        @Override
        public int getDiskMaxUsedSpaceRatio() {
            try {
                Field diskMaxUsedSpaceRatioField = this.getClass().getSuperclass().getDeclaredField("diskMaxUsedSpaceRatio");
                diskMaxUsedSpaceRatioField.setAccessible(true);
                int ratio = (int) diskMaxUsedSpaceRatioField.get(this);
                diskMaxUsedSpaceRatioField.setAccessible(false);
                return ratio;
            } catch (Exception ignored) {
            }
            return super.getDiskMaxUsedSpaceRatio();
        }
    }
}
