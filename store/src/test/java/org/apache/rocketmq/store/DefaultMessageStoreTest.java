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

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMessageStoreTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @Test(expected = OverlappingFileLockException.class)
    public void test_repeat_restart() throws Exception {
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        MessageStore master = new DefaultMessageStore(messageStoreConfig, null, new MyMessageArrivingListener(), new BrokerConfig());

        boolean load = master.load();
        assertTrue(load);

        try {
            master.start();
            master.start();
        } finally {
            master.shutdown();
            master.destroy();
        }
    }

    @After
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new MyMessageArrivingListener(), new BrokerConfig());
    }

    @Test
    public void testWriteAndRead() {
        long ipv4HostMsgs = 10;
        long ipv6HostMsgs = 10;
        long totalMsgs = ipv4HostMsgs + ipv6HostMsgs;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        for (long i = 0; i < ipv4HostMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < ipv6HostMsgs; i++) {
            messageStore.putMessage(buildIPv6HostMessage());
        }

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMsgs, messageStore);
    }

    @Test
    public void testLookMessageByOffset_OffsetIsFirst() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        int firstOffset = 0;
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        AppendMessageResult firstResult = appendMessageResultArray[0];

        MessageExt messageExt = messageStore.lookMessageByOffset(firstResult.getWroteOffset());
        MessageExt messageExt1 = getDefaultMessageStore().lookMessageByOffset(firstResult.getWroteOffset(), firstResult.getWroteBytes());

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, firstOffset));
        assertThat(new String(messageExt1.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, firstOffset));
    }

    @Test
    public void testLookMessageByOffset_OffsetIsLast() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        int lastIndex = totalCount - 1;
        AppendMessageResult lastResult = appendMessageResultArray[lastIndex];

        MessageExt messageExt = getDefaultMessageStore().lookMessageByOffset(lastResult.getWroteOffset(), lastResult.getWroteBytes());

        assertThat(new String(messageExt.getBody())).isEqualTo(buildMessageBodyByOffset(StoreMessage, lastIndex));
    }

    @Test
    public void testLookMessageByOffset_OffsetIsOutOfBound() {
        final int totalCount = 10;
        int queueId = new Random().nextInt(10);
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResultArray = putMessages(totalCount, topic, queueId);
        long lastOffset = getMaxOffset(appendMessageResultArray);

        MessageExt messageExt = getDefaultMessageStore().lookMessageByOffset(lastOffset);

        assertThat(messageExt).isNull();
    }

    @Test
    public void testGetOffsetInQueueByTime() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp());
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(offset);
            assertThat(indexBuffer.getByteBuffer().getLong()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(indexBuffer.getByteBuffer().getInt()).isEqualTo(appendMessageResult.getWroteBytes());
            indexBuffer.release();
        }
    }

    @Test
    public void testGetOffsetInQueueByTime_TimestampIsSkewing() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        int skewing = 2;

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() + skewing);
            long offset2 = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() - skewing);
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(offset);
            SelectMappedBufferResult indexBuffer2 = consumeQueue.getIndexBuffer(offset2);
            assertThat(indexBuffer.getByteBuffer().getLong()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(indexBuffer.getByteBuffer().getInt()).isEqualTo(appendMessageResult.getWroteBytes());
            assertThat(indexBuffer2.getByteBuffer().getLong()).isEqualTo(appendMessageResult.getWroteOffset());
            assertThat(indexBuffer2.getByteBuffer().getInt()).isEqualTo(appendMessageResult.getWroteBytes());
            indexBuffer.release();
            indexBuffer2.release();
        }
    }

    @Test
    public void testGetOffsetInQueueByTime_TimestampSkewingIsLarge() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        int skewing = 20000;

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        for (AppendMessageResult appendMessageResult : appendMessageResults) {
            long offset = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() + skewing);
            long offset2 = messageStore.getOffsetInQueueByTime(topic, queueId, appendMessageResult.getStoreTimestamp() - skewing);
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(offset);
            SelectMappedBufferResult indexBuffer2 = consumeQueue.getIndexBuffer(offset2);
            assertThat(indexBuffer.getByteBuffer().getLong()).isEqualTo(appendMessageResults[totalCount - 1].getWroteOffset());
            assertThat(indexBuffer.getByteBuffer().getInt()).isEqualTo(appendMessageResults[totalCount - 1].getWroteBytes());
            assertThat(indexBuffer2.getByteBuffer().getLong()).isEqualTo(appendMessageResults[0].getWroteOffset());
            assertThat(indexBuffer2.getByteBuffer().getInt()).isEqualTo(appendMessageResults[0].getWroteBytes());

            indexBuffer.release();
            indexBuffer2.release();
        }
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueNotFound1() {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long offset = messageStore.getOffsetInQueueByTime(topic, wrongQueueId, appendMessageResults[0].getStoreTimestamp());

        assertThat(offset).isEqualTo(0);
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueNotFound2() {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, 0);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void testGetOffsetInQueueByTime_ConsumeQueueOffsetNotExist() {
        final int totalCount = 10;
        int queueId = 0;
        int wrongQueueId = 1;
        String topic = "FooBar";
        putMessages(totalCount, topic, queueId, true);
        //Thread.sleep(10);

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, wrongQueueId, -1);

        assertThat(messageStoreTimeStamp).isEqualTo(-1);
    }

    @Test
    public void testGetMessageStoreTimeStamp() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        ConsumeQueue consumeQueue = getDefaultMessageStore().findConsumeQueue(topic, queueId);
        int minOffsetInQueue = (int) consumeQueue.getMinOffsetInQueue();
        for (int i = minOffsetInQueue; i < consumeQueue.getMaxOffsetInQueue(); i++) {
            long messageStoreTimeStamp = messageStore.getMessageStoreTimeStamp(topic, queueId, i);
            assertThat(messageStoreTimeStamp).isEqualTo(appendMessageResults[i].getStoreTimestamp());
        }
    }

    @Test
    public void testGetStoreTime_ParamIsNull() {
        long storeTime = getStoreTime(null);

        assertThat(storeTime).isEqualTo(-1);
    }

    @Test
    public void testGetStoreTime_EverythingIsOk() {
        final int totalCount = 10;
        int queueId = 0;
        String topic = "FooBar";
        AppendMessageResult[] appendMessageResults = putMessages(totalCount, topic, queueId, false);
        //Thread.sleep(10);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        ConsumeQueue consumeQueue = messageStore.getConsumeQueue(topic, queueId);

        for (int i = 0; i < totalCount; i++) {
            SelectMappedBufferResult indexBuffer = consumeQueue.getIndexBuffer(i);
            long storeTime = getStoreTime(indexBuffer);
            assertThat(storeTime).isEqualTo(appendMessageResults[i].getStoreTimestamp());
            indexBuffer.release();
        }
    }

    @Test
    public void testGetStoreTime_PhyOffsetIsLessThanCommitLogMinOffset() {
        long phyOffset = -10;
        int size = 138;
        ByteBuffer byteBuffer = ByteBuffer.allocate(100);
        byteBuffer.putLong(phyOffset);
        byteBuffer.putInt(size);
        byteBuffer.flip();
        MappedFile mappedFile = mock(MappedFile.class);
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, byteBuffer, size, mappedFile);

        long storeTime = getStoreTime(result);
        result.release();

        assertThat(storeTime).isEqualTo(-1);
    }

    private DefaultMessageStore getDefaultMessageStore() {
        return (DefaultMessageStore) this.messageStore;
    }

    private AppendMessageResult[] putMessages(int totalCount, String topic, int queueId) {
        return putMessages(totalCount, topic, queueId, false);
    }

    private AppendMessageResult[] putMessages(int totalCount, String topic, int queueId, boolean interval) {
        AppendMessageResult[] appendMessageResultArray = new AppendMessageResult[totalCount];
        for (int i = 0; i < totalCount; i++) {
            String messageBody = buildMessageBodyByOffset(StoreMessage, i);

            MessageExtBrokerInner msgInner =
                i < totalCount / 2 ? buildMessage(messageBody.getBytes(), topic) : buildIPv6HostMessage(messageBody.getBytes(), topic);
            msgInner.setQueueId(queueId);
            PutMessageResult result = messageStore.putMessage(msgInner);
            appendMessageResultArray[i] = result.getAppendMessageResult();
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
            if (interval) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread sleep ERROR");
                }
            }
        }
        return appendMessageResultArray;
    }

    private long getMaxOffset(AppendMessageResult[] appendMessageResultArray) {
        if (appendMessageResultArray == null) {
            return 0;
        }
        AppendMessageResult last = appendMessageResultArray[appendMessageResultArray.length - 1];
        return last.getWroteOffset() + last.getWroteBytes();
    }

    private String buildMessageBodyByOffset(String message, long i) {
        return String.format("%s offset %d", message, i);
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        try {
            Method getStoreTime = getDefaultMessageStore().getClass().getDeclaredMethod("getStoreTime", SelectMappedBufferResult.class);
            getStoreTime.setAccessible(true);
            return (long) getStoreTime.invoke(getDefaultMessageStore(), result);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private MessageExtBrokerInner buildMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

    private MessageExtBrokerInner buildIPv6HostMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornTimestamp(System.currentTimeMillis());
        try {
            msg.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        try {
            msg.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        return msg;
    }

    private MessageExtBrokerInner buildMessage() {
        return buildMessage(MessageBody, "FooBar");
    }

    private MessageExtBrokerInner buildIPv6HostMessage() {
        return buildIPv6HostMessage(MessageBody, "FooBar");
    }

    private void verifyThatMasterIsFunctional(long totalMsgs, MessageStore master) {
        for (long i = 0; i < totalMsgs; i++) {
            master.putMessage(buildMessage());
        }

        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();

        }
    }

    @Test
    public void testPullSize() throws Exception {
        String topic = "pullSizeTopic";

        for (int i = 0; i < 32; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        // wait for consume queue build
        // the sleep time should be great than consume queue flush interval
        //Thread.sleep(100);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        String group = "simple";
        GetMessageResult getMessageResult32 = messageStore.getMessage(group, topic, 0, 0, 32, null);
        assertThat(getMessageResult32.getMessageBufferList().size()).isEqualTo(32);
        getMessageResult32.release();

        GetMessageResult getMessageResult20 = messageStore.getMessage(group, topic, 0, 0, 20, null);
        assertThat(getMessageResult20.getMessageBufferList().size()).isEqualTo(20);

        getMessageResult20.release();
        GetMessageResult getMessageResult45 = messageStore.getMessage(group, topic, 0, 0, 10, null);
        assertThat(getMessageResult45.getMessageBufferList().size()).isEqualTo(10);
        getMessageResult45.release();

    }

    @Test
    public void testRecover() throws Exception {
        String topic = "recoverTopic";
        MessageBody = StoreMessage.getBytes();
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }

        // Thread.sleep(100);//wait for build consumer queue
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        long maxPhyOffset = messageStore.getMaxPhyOffset();
        long maxCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        //1.just reboot
        messageStore.shutdown();
        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(maxPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(maxCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //2.damage commitlog and reboot normal
        for (int i = 0; i < 100; i++) {
            MessageExtBrokerInner messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        //Thread.sleep(100);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        long secondLastPhyOffset = messageStore.getMaxPhyOffset();
        long secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        MessageExtBrokerInner messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);

        messageStore.shutdown();

        //damage last message
        damageCommitlog(secondLastPhyOffset);

        //reboot
        messageStore = buildMessageStore();
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(secondLastPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(secondLastCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //3.damage commitlog and reboot abnormal
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
        //Thread.sleep(100);
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        secondLastPhyOffset = messageStore.getMaxPhyOffset();
        secondLastCqOffset = messageStore.getMaxOffsetInQueue(topic, 0);

        messageExtBrokerInner = buildMessage();
        messageExtBrokerInner.setTopic(topic);
        messageExtBrokerInner.setQueueId(0);
        messageStore.putMessage(messageExtBrokerInner);
        messageStore.shutdown();

        //damage last message
        damageCommitlog(secondLastPhyOffset);
        //add abort file
        String fileName = StorePathConfigHelper.getAbortFile(((DefaultMessageStore) messageStore).getMessageStoreConfig().getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        file.createNewFile();

        messageStore = buildMessageStore();
        load = messageStore.load();
        assertTrue(load);
        messageStore.start();
        assertTrue(secondLastPhyOffset == messageStore.getMaxPhyOffset());
        assertTrue(secondLastCqOffset == messageStore.getMaxOffsetInQueue(topic, 0));

        //message write again
        for (int i = 0; i < 100; i++) {
            messageExtBrokerInner = buildMessage();
            messageExtBrokerInner.setTopic(topic);
            messageExtBrokerInner.setQueueId(0);
            messageStore.putMessage(messageExtBrokerInner);
        }
    }

    private void damageCommitlog(long offset) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathCommitLog() + File.separator + "00000000000000000000");

        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 10);

        int bodyLen = mappedByteBuffer.getInt((int) offset + 84);
        int topicLenIndex = (int) offset + 84 + bodyLen + 4;
        mappedByteBuffer.position(topicLenIndex);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);
        mappedByteBuffer.putInt(0);

        mappedByteBuffer.force();
        fileChannel.force(true);
        fileChannel.close();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
