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

package org.apache.rocketmq.common.message;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.rocketmq.common.message.MessageDecoder.createMessageId;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageDecoderTest {

    @Test
    public void testDecodeProperties() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("645100FA00002A9F000000489A3AA09E");
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

        assertThat(properties).isNotNull();
        assertThat("123").isEqualTo(properties.get("a"));
        assertThat("hello").isEqualTo(properties.get("b"));
        assertThat("3.14").isEqualTo(properties.get("c"));
    }

    @Test
    public void testDecodePropertiesOnIPv6Host() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExt.setBornHostV6Flag();
        messageExt.setStoreHostAddressV6Flag();
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

        assertThat(properties).isNotNull();
        assertThat("123").isEqualTo(properties.get("a"));
        assertThat("hello").isEqualTo(properties.get("b"));
        assertThat("3.14").isEqualTo(properties.get("c"));
    }

    @Test
    public void testEncodeAndDecode() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("645100FA00002A9F000000489A3AA09E");
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(1);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        byteBuffer.clear();
        MessageExt decodedMsg = MessageDecoder.decode(byteBuffer);

        assertThat(decodedMsg).isNotNull();
        assertThat(1).isEqualTo(decodedMsg.getQueueId());
        assertThat(123456L).isEqualTo(decodedMsg.getCommitLogOffset());
        assertThat("hello!q!".getBytes()).isEqualTo(decodedMsg.getBody());

        int msgIDLength = 4 + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        String msgId = createMessageId(byteBufferMsgId, messageExt.getStoreHostBytes(), messageExt.getCommitLogOffset());
        assertThat(msgId).isEqualTo(decodedMsg.getMsgId());

        assertThat("abc").isEqualTo(decodedMsg.getTopic());
    }

    @Test
    public void testEncodeAndDecodeOnIPv6Host() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        messageExt.setBornHostV6Flag();
        messageExt.setStoreHostAddressV6Flag();
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(1);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        byteBuffer.clear();
        MessageExt decodedMsg = MessageDecoder.decode(byteBuffer);

        assertThat(decodedMsg).isNotNull();
        assertThat(1).isEqualTo(decodedMsg.getQueueId());
        assertThat(123456L).isEqualTo(decodedMsg.getCommitLogOffset());
        assertThat("hello!q!".getBytes()).isEqualTo(decodedMsg.getBody());
        assertThat(48).isEqualTo(decodedMsg.getSysFlag());

        int msgIDLength = 16 + 4 + 8;
        ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
        String msgId = createMessageId(byteBufferMsgId, messageExt.getStoreHostBytes(), messageExt.getCommitLogOffset());
        assertThat(msgId).isEqualTo(decodedMsg.getMsgId());

        assertThat("abc").isEqualTo(decodedMsg.getTopic());
    }
}
