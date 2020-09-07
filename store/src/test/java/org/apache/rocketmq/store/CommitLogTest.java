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
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.store.dledger.MessageStoreTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class CommitLogTest extends MessageStoreTestBase {
    private DefaultMessageStore originalStore;
    private String topic;
    private String base;

    @Before
    public void setUp() throws Exception {
        base = createBaseDir();
        topic = UUID.randomUUID().toString();
        originalStore = createMessageStore(base, false);
    }

    @After
    public void tearDown() {
        originalStore.shutdown();
        UtilAll.deleteFile(new File(base));
    }

    @Test
    public void putMessage() throws Exception {

        doPutMessages(originalStore, topic, 0, 1000, 0);
        Thread.sleep(500);
        Assert.assertEquals(0, originalStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(1000, originalStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, originalStore.dispatchBehindBytes());

        long startDeliverTime = System.currentTimeMillis() / 1000 * 1000;
        doPutDelayMsg(originalStore, topic, 0, 500, 1000, startDeliverTime);
        Thread.sleep(500);
        Assert.assertEquals(1000, originalStore.getMaxOffsetInQueue(topic, 0));
    }

    private void doPutDelayMsg(MessageStore messageStore, String topic, int queueId, int num, long beginLogicsOffset, long startDeliverTime) {
        for (int i = 0; i < num; i++) {
            MessageExtBrokerInner msgInner = buildMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(queueId);
            msgInner.getProperties().put(MessageConst.PROPERTY_START_DELIVER_TIME, String.valueOf(startDeliverTime));
            PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
    }
}