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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageStoreWithFilterTest {

    private static final String msg = "Once, there was a chance for me!";
    private static final byte[] msgBody = msg.getBytes();

    private static final String topic = "topic";
    private static final int queueId = 0;
    private static final String storePath = "." + File.separator + "unit_test_store";
    private static final int commitLogFileSize = 1024 * 1024 * 256;
    private static final int cqFileSize = 300000 * 20;
    private static final int cqExtFileSize = 300000 * 128;

    private static SocketAddress BornHost;

    private static SocketAddress StoreHost;

    private DefaultMessageStore master;

    private ConsumerFilterManager filterManager;

    private int topicCount = 3;

    private int msgPerTopic = 30;

    static {
        try {
            StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
        }
        try {
            BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
        }
    }

    @Before
    public void init() throws Exception {
        filterManager = ConsumerFilterManagerTest.gen(topicCount, msgPerTopic);
        master = gen(filterManager);
    }

    @After
    public void destroy() {
        master.shutdown();
        master.destroy();
        UtilAll.deleteFile(new File(storePath));
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags(System.currentTimeMillis() + "TAG");
        msg.setKeys("Hello");
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(queueId);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        for (int i = 1; i < 3; i++) {
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    public MessageStoreConfig buildStoreConfig(int commitLogFileSize, int cqFileSize,
                                               boolean enableCqExt, int cqExtFileSize) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(enableCqExt);

        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        return messageStoreConfig;
    }

    protected DefaultMessageStore gen(ConsumerFilterManager filterManager) throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
            commitLogFileSize, cqFileSize, true, cqExtFileSize
        );

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableCalcFilterBitMap(true);
        brokerConfig.setMaxErrorRateOfBloomFilter(20);
        brokerConfig.setExpectConsumerNumUseFilter(64);

        DefaultMessageStore master = new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager(brokerConfig.getBrokerClusterName()),
            new MessageArrivingListener() {
                @Override
                public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                                     long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
                }
            }
            , brokerConfig);

        master.getDispatcherList().addFirst(new CommitLogDispatcher() {
            @Override
            public void dispatch(DispatchRequest request) {
                try {
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        master.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(brokerConfig, filterManager));

        assertThat(master.load()).isTrue();

        master.start();

        return master;
    }

    protected List<MessageExtBrokerInner> putMsg(DefaultMessageStore master, int topicCount,
                                                 int msgCountPerTopic) throws Exception {
        List<MessageExtBrokerInner> msgs = new ArrayList<MessageExtBrokerInner>();
        for (int i = 0; i < topicCount; i++) {
            String realTopic = topic + i;
            for (int j = 0; j < msgCountPerTopic; j++) {
                MessageExtBrokerInner msg = buildMessage();
                msg.setTopic(realTopic);
                msg.putUserProperty("a", String.valueOf(j * 10 + 5));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                PutMessageResult result = master.putMessage(msg);

                msg.setMsgId(result.getAppendMessageResult().getMsgId());

                msgs.add(msg);
            }
        }

        return msgs;
    }

    protected List<MessageExtBrokerInner> filtered(List<MessageExtBrokerInner> msgs, ConsumerFilterData filterData) {
        List<MessageExtBrokerInner> filteredMsgs = new ArrayList<MessageExtBrokerInner>();

        for (MessageExtBrokerInner messageExtBrokerInner : msgs) {

            if (!messageExtBrokerInner.getTopic().equals(filterData.getTopic())) {
                continue;
            }

            try {
                Object evlRet = filterData.getCompiledExpression().evaluate(new MessageEvaluationContext(messageExtBrokerInner.getProperties()));

                if (evlRet == null || !(evlRet instanceof Boolean) || (Boolean) evlRet) {
                    filteredMsgs.add(messageExtBrokerInner);
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertThat(true).isFalse();
            }
        }

        return filteredMsgs;
    }

    @Test
    public void testGetMessage_withFilterBitMapAndConsumerChanged() throws Exception {
        List<MessageExtBrokerInner> msgs = putMsg(master, topicCount, msgPerTopic);

        Thread.sleep(200);

        // reset consumer;
        String topic = "topic" + 0;
        String resetGroup = "CID_" + 2;
        String normalGroup = "CID_" + 3;

        {
            // reset CID_2@topic0 to get all messages.
            SubscriptionData resetSubData = new SubscriptionData();
            resetSubData.setExpressionType(ExpressionType.SQL92);
            resetSubData.setTopic(topic);
            resetSubData.setClassFilterMode(false);
            resetSubData.setSubString("a is not null OR a is null");

            ConsumerFilterData resetFilterData = ConsumerFilterManager.build(topic,
                resetGroup, resetSubData.getSubString(), resetSubData.getExpressionType(),
                System.currentTimeMillis());

            GetMessageResult resetGetResult = master.getMessage(resetGroup, topic, queueId, 0, 1000,
                new ExpressionMessageFilter(resetSubData, resetFilterData, filterManager));

            try {
                assertThat(resetGetResult).isNotNull();

                List<MessageExtBrokerInner> filteredMsgs = filtered(msgs, resetFilterData);

                assertThat(resetGetResult.getMessageBufferList().size()).isEqualTo(filteredMsgs.size());
            } finally {
                resetGetResult.release();
            }
        }

        {
            ConsumerFilterData normalFilterData = filterManager.get(topic, normalGroup);
            assertThat(normalFilterData).isNotNull();
            assertThat(normalFilterData.getBornTime()).isLessThan(System.currentTimeMillis());

            SubscriptionData normalSubData = new SubscriptionData();
            normalSubData.setExpressionType(normalFilterData.getExpressionType());
            normalSubData.setTopic(topic);
            normalSubData.setClassFilterMode(false);
            normalSubData.setSubString(normalFilterData.getExpression());

            List<MessageExtBrokerInner> filteredMsgs = filtered(msgs, normalFilterData);

            GetMessageResult normalGetResult = master.getMessage(normalGroup, topic, queueId, 0, 1000,
                new ExpressionMessageFilter(normalSubData, normalFilterData, filterManager));

            try {
                assertThat(normalGetResult).isNotNull();
                assertThat(normalGetResult.getMessageBufferList().size()).isEqualTo(filteredMsgs.size());
            } finally {
                normalGetResult.release();
            }
        }
    }

    @Test
    public void testGetMessage_withFilterBitMap() throws Exception {
        List<MessageExtBrokerInner> msgs = putMsg(master, topicCount, msgPerTopic);

        Thread.sleep(100);

        for (int i = 0; i < topicCount; i++) {
            String realTopic = topic + i;

            for (int j = 0; j < msgPerTopic; j++) {
                String group = "CID_" + j;

                ConsumerFilterData filterData = filterManager.get(realTopic, group);
                assertThat(filterData).isNotNull();

                List<MessageExtBrokerInner> filteredMsgs = filtered(msgs, filterData);

                SubscriptionData subscriptionData = new SubscriptionData();
                subscriptionData.setExpressionType(filterData.getExpressionType());
                subscriptionData.setTopic(filterData.getTopic());
                subscriptionData.setClassFilterMode(false);
                subscriptionData.setSubString(filterData.getExpression());

                GetMessageResult getMessageResult = master.getMessage(group, realTopic, queueId, 0, 10000,
                    new ExpressionMessageFilter(subscriptionData, filterData, filterManager));
                String assertMsg = group + "-" + realTopic;
                try {
                    assertThat(getMessageResult).isNotNull();
                    assertThat(GetMessageStatus.FOUND).isEqualTo(getMessageResult.getStatus());
                    assertThat(getMessageResult.getMessageBufferList()).isNotNull().isNotEmpty();
                    assertThat(getMessageResult.getMessageBufferList().size()).isEqualTo(filteredMsgs.size());

                    for (ByteBuffer buffer : getMessageResult.getMessageBufferList()) {
                        MessageExt messageExt = MessageDecoder.decode(buffer.slice(), false);
                        assertThat(messageExt).isNotNull();

                        Object evlRet = null;
                        try {
                            evlRet = filterData.getCompiledExpression().evaluate(new MessageEvaluationContext(messageExt.getProperties()));
                        } catch (Exception e) {
                            e.printStackTrace();
                            assertThat(true).isFalse();
                        }

                        assertThat(evlRet).isNotNull().isEqualTo(Boolean.TRUE);

                        // check
                        boolean find = false;
                        for (MessageExtBrokerInner messageExtBrokerInner : filteredMsgs) {
                            if (messageExtBrokerInner.getMsgId().equals(messageExt.getMsgId())) {
                                find = true;
                            }
                        }
                        assertThat(find).isTrue();
                    }
                } finally {
                    getMessageResult.release();
                }
            }
        }
    }

    @Test
    public void testGetMessage_withFilter_checkTagsCode() throws Exception {
        putMsg(master, topicCount, msgPerTopic);

        Thread.sleep(200);

        for (int i = 0; i < topicCount; i++) {
            String realTopic = topic + i;

            GetMessageResult getMessageResult = master.getMessage("test", realTopic, queueId, 0, 10000,
                new MessageFilter() {
                    @Override
                    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                        if (tagsCode != null && tagsCode <= ConsumeQueueExt.MAX_ADDR) {
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                        return true;
                    }
                });
            assertThat(getMessageResult.getMessageCount()).isEqualTo(msgPerTopic);
        }
    }
}
