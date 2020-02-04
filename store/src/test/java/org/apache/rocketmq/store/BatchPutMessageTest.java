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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class BatchPutMessageTest {

    private MessageStore messageStore;

    public static final char NAME_VALUE_SEPARATOR = 1;
    public static final char PROPERTY_SEPARATOR = 2;
    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    @Before
    public void init() throws Exception {
        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @After
    public void destory() {
        messageStore.shutdown();
        messageStore.destroy();

        UtilAll.deleteFile(new File(System.getProperty("user.home") + File.separator + "putmessagesteststore"));
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 4);
        messageStoreConfig.setMaxHashSlotNum(100);
        messageStoreConfig.setMaxIndexNum(100 * 10);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setStorePathRootDir(System.getProperty("user.home") + File.separator + "putmessagesteststore");
        messageStoreConfig.setStorePathCommitLog(System.getProperty("user.home") + File.separator + "putmessagesteststore" + File.separator + "commitlog");
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new MyMessageArrivingListener(), new BrokerConfig());
    }

    @Test
    public void testPutMessages() throws Exception {
        List<Message> messages = new ArrayList<>();
        String topic = "batch-write-topic";
        int queue = 0;
        int[] msgLengthArr = new int[11];
        msgLengthArr[0] = 0;
        int j = 1;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody(("body" + i).getBytes());
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            messages.add(msg);
            String properties = messageProperties2String(msg.getProperties());
            byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
            short propertiesLength = (short) propertiesBytes.length;
            final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;
            msgLengthArr[j] = calMsgLength(msg.getBody().length, topicLength, propertiesLength) + msgLengthArr[j - 1];
            j++;
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBody(batchMessageBody);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 126));

        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        assertThat(putMessageResult.isOk()).isTrue();
        
        Thread.sleep(3 * 1000);

        for (long i = 0; i < 10; i++) {
            MessageExt messageExt = messageStore.lookMessageByOffset(msgLengthArr[(int) i]);
            assertThat(messageExt).isNotNull();
            GetMessageResult result = messageStore.getMessage("batch_write_group", topic, queue, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            assertThat(result.getStatus()).isEqualTo(GetMessageStatus.FOUND);
            result.release();
        }

    }

    @Test
    public void testPutIPv6HostMessages() throws Exception {
        List<Message> messages = new ArrayList<>();
        String topic = "batch-write-topic";
        int queue = 0;
        int[] msgLengthArr = new int[11];
        msgLengthArr[0] = 0;
        int j = 1;
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setBody(("body" + i).getBytes());
            msg.setTopic(topic);
            msg.setTags("TAG1");
            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            messages.add(msg);
            String properties = messageProperties2String(msg.getProperties());
            byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
            short propertiesLength = (short) propertiesBytes.length;
            final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;
            msgLengthArr[j] = calIPv6HostMsgLength(msg.getBody().length, topicLength, propertiesLength) + msgLengthArr[j - 1];
            j++;
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(queue);
        messageExtBatch.setBody(batchMessageBody);
        messageExtBatch.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setSysFlag(0);
        messageExtBatch.setBornHostV6Flag();
        messageExtBatch.setStoreHostAddressV6Flag();
        messageExtBatch.setStoreHost(new InetSocketAddress("1050:0000:0000:0000:0005:0600:300c:326b", 125));
        messageExtBatch.setBornHost(new InetSocketAddress("::1", 126));

        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        assertThat(putMessageResult.isOk()).isTrue();

        Thread.sleep(3 * 1000);

        for (long i = 0; i < 10; i++) {
            MessageExt messageExt = messageStore.lookMessageByOffset(msgLengthArr[(int) i]);
            assertThat(messageExt).isNotNull();
            GetMessageResult result = messageStore.getMessage("batch_write_group", topic, queue, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            assertThat(result.getStatus()).isEqualTo(GetMessageStatus.FOUND);
            result.release();
        }

    }

    private int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + 8 //BORNHOST
                + 8 //STORETIMESTAMP
                + 8 //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    private int calIPv6HostMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
            + 4 //MAGICCODE
            + 4 //BODYCRC
            + 4 //QUEUEID
            + 4 //FLAG
            + 8 //QUEUEOFFSET
            + 8 //PHYSICALOFFSET
            + 4 //SYSFLAG
            + 8 //BORNTIMESTAMP
            + 20 //BORNHOST
            + 8 //STORETIMESTAMP
            + 20 //STOREHOSTADDRESS
            + 4 //RECONSUMETIMES
            + 8 //Prepared Transaction Offset
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
            + 1 + topicLength //TOPIC
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
            + 0;
        return msgLen;
    }

    public String messageProperties2String(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        if (properties != null) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                final String name = entry.getKey();
                final String value = entry.getValue();

                sb.append(name);
                sb.append(NAME_VALUE_SEPARATOR);
                sb.append(value);
                sb.append(PROPERTY_SEPARATOR);
            }
        }
        return sb.toString();
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
