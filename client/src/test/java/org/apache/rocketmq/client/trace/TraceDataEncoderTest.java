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

package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.MessageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TraceDataEncoderTest {

    private String traceData;

    private long time;

    @Before
    public void init() {
        time = System.currentTimeMillis();
        traceData = new StringBuilder()
            .append("Pub").append(TraceConstants.CONTENT_SPLITOR)
            .append(time).append(TraceConstants.CONTENT_SPLITOR)
            .append("DefaultRegion").append(TraceConstants.CONTENT_SPLITOR)
            .append("PID-test").append(TraceConstants.CONTENT_SPLITOR)
            .append("topic-test").append(TraceConstants.CONTENT_SPLITOR)
            .append("AC1415116D1418B4AAC217FE1B4E0000").append(TraceConstants.CONTENT_SPLITOR)
            .append("Tags").append(TraceConstants.CONTENT_SPLITOR)
            .append("Keys").append(TraceConstants.CONTENT_SPLITOR)
            .append("127.0.0.1:10911").append(TraceConstants.CONTENT_SPLITOR)
            .append(26).append(TraceConstants.CONTENT_SPLITOR)
            .append(245).append(TraceConstants.CONTENT_SPLITOR)
            .append(MessageType.Normal_Msg.ordinal()).append(TraceConstants.CONTENT_SPLITOR)
            .append("0A9A002600002A9F0000000000002329").append(TraceConstants.CONTENT_SPLITOR)
            .append(true).append(TraceConstants.FIELD_SPLITOR)
            .toString();
    }

    @Test
    public void testDecoderFromTraceDataString() {
        List<TraceContext> contexts = TraceDataEncoder.decoderFromTraceDataString(traceData);
        Assert.assertEquals(contexts.size(), 1);
        Assert.assertEquals(contexts.get(0).getTraceType(), TraceType.Pub);
    }

    @Test
    public void testEncoderFromContextBean() {
        TraceContext context = new TraceContext();
        context.setTraceType(TraceType.Pub);
        context.setGroupName("PID-test");
        context.setRegionId("DefaultRegion");
        context.setCostTime(245);
        context.setSuccess(true);
        context.setTimeStamp(time);
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic("topic-test");
        traceBean.setKeys("Keys");
        traceBean.setTags("Tags");
        traceBean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        traceBean.setOffsetMsgId("0A9A002600002A9F0000000000002329");
        traceBean.setStoreHost("127.0.0.1:10911");
        traceBean.setStoreTime(time);
        traceBean.setMsgType(MessageType.Normal_Msg);
        traceBean.setBodyLength(26);
        List<TraceBean> traceBeans = new ArrayList<TraceBean>();
        traceBeans.add(traceBean);
        context.setTraceBeans(traceBeans);
        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(context);

        Assert.assertEquals(traceTransferBean.getTransData(), traceData);
        Assert.assertEquals(traceTransferBean.getTransKey().size(), 2);
    }

    @Test
    public void testEncoderFromContextBean_EndTransaction() {
        TraceContext context = new TraceContext();
        context.setTraceType(TraceType.EndTransaction);
        context.setGroupName("PID-test");
        context.setRegionId("DefaultRegion");
        context.setTimeStamp(time);
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic("topic-test");
        traceBean.setKeys("Keys");
        traceBean.setTags("Tags");
        traceBean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        traceBean.setStoreHost("127.0.0.1:10911");
        traceBean.setMsgType(MessageType.Trans_msg_Commit);
        traceBean.setTransactionId("transactionId");
        traceBean.setTransactionState(LocalTransactionState.COMMIT_MESSAGE);
        traceBean.setFromTransactionCheck(false);
        List<TraceBean> traceBeans = new ArrayList<TraceBean>();
        traceBeans.add(traceBean);
        context.setTraceBeans(traceBeans);
        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(context);

        Assert.assertEquals(traceTransferBean.getTransKey().size(), 2);
        String traceData = traceTransferBean.getTransData();
        TraceContext contextAfter = TraceDataEncoder.decoderFromTraceDataString(traceData).get(0);
        Assert.assertEquals(context.getTraceType(), contextAfter.getTraceType());
        Assert.assertEquals(context.getTimeStamp(), contextAfter.getTimeStamp());
        Assert.assertEquals(context.getGroupName(), contextAfter.getGroupName());
        TraceBean before = context.getTraceBeans().get(0);
        TraceBean after = contextAfter.getTraceBeans().get(0);
        Assert.assertEquals(before.getTopic(), after.getTopic());
        Assert.assertEquals(before.getMsgId(), after.getMsgId());
        Assert.assertEquals(before.getTags(), after.getTags());
        Assert.assertEquals(before.getKeys(), after.getKeys());
        Assert.assertEquals(before.getStoreHost(), after.getStoreHost());
        Assert.assertEquals(before.getMsgType(), after.getMsgType());
        Assert.assertEquals(before.getClientHost(), after.getClientHost());
        Assert.assertEquals(before.getTransactionId(), after.getTransactionId());
        Assert.assertEquals(before.getTransactionState(), after.getTransactionState());
        Assert.assertEquals(before.isFromTransactionCheck(), after.isFromTransactionCheck());
    }
    
    @Test
    public void testPubTraceDataFormatTest() {
        TraceContext pubContext = new TraceContext();
        pubContext.setTraceType(TraceType.Pub);
        pubContext.setTimeStamp(time);
        pubContext.setRegionId("Default-region");
        pubContext.setGroupName("GroupName-test");
        pubContext.setCostTime(34);
        pubContext.setSuccess(true);
        TraceBean bean = new TraceBean();
        bean.setTopic("topic-test");
        bean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        bean.setTags("tags");
        bean.setKeys("keys");
        bean.setStoreHost("127.0.0.1:10911");
        bean.setBodyLength(100);
        bean.setMsgType(MessageType.Normal_Msg);
        bean.setOffsetMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        pubContext.setTraceBeans(new ArrayList<TraceBean>(1));
        pubContext.getTraceBeans().add(bean);

        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(pubContext);
        String transData = traceTransferBean.getTransData();
        Assert.assertNotNull(transData);
        String[] items = transData.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
        Assert.assertEquals(14, items.length);

    }

    @Test
    public void testSubBeforeTraceDataFormatTest() {
        TraceContext subBeforeContext = new TraceContext();
        subBeforeContext.setTraceType(TraceType.SubBefore);
        subBeforeContext.setTimeStamp(time);
        subBeforeContext.setRegionId("Default-region");
        subBeforeContext.setGroupName("GroupName-test");
        subBeforeContext.setRequestId("3455848576927");
        TraceBean bean = new TraceBean();
        bean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        bean.setRetryTimes(0);
        bean.setKeys("keys");
        subBeforeContext.setTraceBeans(new ArrayList<TraceBean>(1));
        subBeforeContext.getTraceBeans().add(bean);

        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(subBeforeContext);
        String transData = traceTransferBean.getTransData();
        Assert.assertNotNull(transData);
        String[] items = transData.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
        Assert.assertEquals(8, items.length);

    }

    @Test
    public void testSubAfterTraceDataFormatTest() {
        TraceContext subAfterContext = new TraceContext();
        subAfterContext.setTraceType(TraceType.SubAfter);
        subAfterContext.setRequestId("3455848576927");
        subAfterContext.setCostTime(20);
        subAfterContext.setSuccess(true);
        subAfterContext.setTimeStamp(1625883640000L);
        subAfterContext.setContextCode(98623046);
        TraceBean bean = new TraceBean();
        bean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        bean.setKeys("keys");
        subAfterContext.setTraceBeans(new ArrayList<TraceBean>(1));
        subAfterContext.getTraceBeans().add(bean);

        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(subAfterContext);
        String transData = traceTransferBean.getTransData();
        Assert.assertNotNull(transData);
        String[] items = transData.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
        Assert.assertEquals(8, items.length);

    }

    @Test
    public void testEndTrxTraceDataFormatTest() {
        TraceContext endTrxContext = new TraceContext();
        endTrxContext.setTraceType(TraceType.EndTransaction);
        endTrxContext.setGroupName("PID-test");
        endTrxContext.setRegionId("DefaultRegion");
        endTrxContext.setTimeStamp(time);
        TraceBean endTrxTraceBean = new TraceBean();
        endTrxTraceBean.setTopic("topic-test");
        endTrxTraceBean.setKeys("Keys");
        endTrxTraceBean.setTags("Tags");
        endTrxTraceBean.setMsgId("AC1415116D1418B4AAC217FE1B4E0000");
        endTrxTraceBean.setStoreHost("127.0.0.1:10911");
        endTrxTraceBean.setMsgType(MessageType.Trans_msg_Commit);
        endTrxTraceBean.setTransactionId("transactionId");
        endTrxTraceBean.setTransactionState(LocalTransactionState.COMMIT_MESSAGE);
        endTrxTraceBean.setFromTransactionCheck(false);
        List<TraceBean> traceBeans = new ArrayList<TraceBean>();
        traceBeans.add(endTrxTraceBean);
        endTrxContext.setTraceBeans(traceBeans);

        TraceTransferBean traceTransferBean = TraceDataEncoder.encoderFromContextBean(endTrxContext);
        String transData = traceTransferBean.getTransData();
        Assert.assertNotNull(transData);
        String[] items = transData.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
        Assert.assertEquals(13, items.length);

    }
}
