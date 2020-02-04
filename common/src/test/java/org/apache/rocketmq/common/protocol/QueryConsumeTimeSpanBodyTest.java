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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryConsumeTimeSpanBodyTest {

    @Test
    public void testSetGet() throws Exception {
        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> firstQueueTimeSpans = newUniqueConsumeTimeSpanSet();
        List<QueueTimeSpan> secondQueueTimeSpans = newUniqueConsumeTimeSpanSet();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(firstQueueTimeSpans);
        assertThat(queryConsumeTimeSpanBody.getConsumeTimeSpanSet()).isEqualTo(firstQueueTimeSpans);
        assertThat(queryConsumeTimeSpanBody.getConsumeTimeSpanSet()).isNotEqualTo(secondQueueTimeSpans);
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(secondQueueTimeSpans);
        assertThat(queryConsumeTimeSpanBody.getConsumeTimeSpanSet()).isEqualTo(secondQueueTimeSpans);
        assertThat(queryConsumeTimeSpanBody.getConsumeTimeSpanSet()).isNotEqualTo(firstQueueTimeSpans);
    }

    @Test
    public void testFromJson() throws Exception {
        QueryConsumeTimeSpanBody qctsb = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = new ArrayList<QueueTimeSpan>();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMinTimeStamp(1550825710000l);
        queueTimeSpan.setMaxTimeStamp(1550825790000l);
        queueTimeSpan.setConsumeTimeStamp(1550825760000l);
        queueTimeSpan.setDelayTime(5000l);
        MessageQueue messageQueue = new MessageQueue("topicName", "brokerName", 1);
        queueTimeSpan.setMessageQueue(messageQueue);
        queueTimeSpans.add(queueTimeSpan);
        qctsb.setConsumeTimeSpanSet(queueTimeSpans);
        String json = RemotingSerializable.toJson(qctsb, true);
        QueryConsumeTimeSpanBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeTimeSpanBody.class);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMaxTimeStamp()).isEqualTo(1550825790000l);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMinTimeStamp()).isEqualTo(1550825710000l);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getDelayTime()).isEqualTo(5000l);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue()).isEqualTo(messageQueue);
    }

    @Test
    public void testFromJsonRandom() throws Exception {
        QueryConsumeTimeSpanBody origin = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = newUniqueConsumeTimeSpanSet();
        origin.setConsumeTimeSpanSet(queueTimeSpans);
        String json = origin.toJson(true);
        QueryConsumeTimeSpanBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeTimeSpanBody.class);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMinTimeStamp()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMinTimeStamp());
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMaxTimeStamp()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMaxTimeStamp());
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp());
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getDelayTime()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getDelayTime());
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName());
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic());
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId());
    }

    @Test
    public void testEncode() throws Exception {
        QueryConsumeTimeSpanBody origin = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = newUniqueConsumeTimeSpanSet();
        origin.setConsumeTimeSpanSet(queueTimeSpans);
        byte[] data = origin.encode();
        QueryConsumeTimeSpanBody fromData = RemotingSerializable.decode(data, QueryConsumeTimeSpanBody.class);
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getMinTimeStamp()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMinTimeStamp());
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getMaxTimeStamp()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMaxTimeStamp());
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp());
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getDelayTime()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getDelayTime());
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName());
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic());
        assertThat(fromData.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId()).isEqualTo(origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId());
    }

    private List<QueueTimeSpan> newUniqueConsumeTimeSpanSet() {
        List<QueueTimeSpan> queueTimeSpans = new ArrayList<QueueTimeSpan>();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMinTimeStamp(System.currentTimeMillis());
        queueTimeSpan.setMaxTimeStamp(UtilAll.computeNextHourTimeMillis());
        queueTimeSpan.setConsumeTimeStamp(UtilAll.computeNextMinutesTimeMillis());
        queueTimeSpan.setDelayTime(5000l);
        MessageQueue messageQueue = new MessageQueue(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Random().nextInt());
        queueTimeSpan.setMessageQueue(messageQueue);
        queueTimeSpans.add(queueTimeSpan);
        return queueTimeSpans;
    }
}
