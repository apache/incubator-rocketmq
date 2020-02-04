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
package org.apache.rocketmq.store.dledger;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFileList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.junit.Assert;
import org.junit.Test;

public class DLedgerCommitlogTest extends MessageStoreTestBase {


    @Test
    public void testTruncateCQ() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Thread.sleep(2000);
            doPutMessages(messageStore, topic, 0, 2000, 0);
            Thread.sleep(100);
            Assert.assertEquals(24, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 2000, 0);
            messageStore.shutdown();
        }

        {
            //Abnormal recover, left some commitlogs
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 4);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Thread.sleep(1000);
            Assert.assertEquals(20, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1700, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1700, 0);
            messageStore.shutdown();
        }
        {
            //Abnormal recover, left none commitlogs
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 20);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
            DLedgerServer dLedgerServer = dLedgerCommitLog.getdLedgerServer();
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore) dLedgerServer.getdLedgerStore();
            MmapFileList mmapFileList = dLedgerMmapFileStore.getDataFileList();
            Thread.sleep(1000);
            Assert.assertEquals(0, mmapFileList.getMappedFiles().size());
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            messageStore.shutdown();
        }
    }



    @Test
    public void testRecover() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        String topic = UUID.randomUUID().toString();
        {
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            Thread.sleep(1000);
            doPutMessages(messageStore, topic, 0, 1000, 0);
            Thread.sleep(100);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //normal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }

        {
            //abnormal recover
            DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, messageStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, messageStore.dispatchBehindBytes());
            doGetMessages(messageStore, topic, 0, 1000, 0);
            messageStore.shutdown();
        }
    }



    @Test
    public void testPutAndGetMessage() throws Exception {
        String base =  createBaseDir();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore messageStore = createDledgerMessageStore(base, group, "n0", peers, null, false, 0);
        Thread.sleep(1000);
        String topic = UUID.randomUUID().toString();

        List<PutMessageResult> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner msgInner =
                i < 5 ? buildMessage() : buildIPv6HostMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(0);
            PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            results.add(putMessageResult);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(i, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
        Thread.sleep(100);
        Assert.assertEquals(0, messageStore.getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(10, messageStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(0, messageStore.dispatchBehindBytes());
        GetMessageResult getMessageResult =  messageStore.getMessage("group", topic, 0, 0, 32, null);
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());

        Assert.assertEquals(10, getMessageResult.getMessageBufferList().size());
        Assert.assertEquals(10, getMessageResult.getMessageMapedList().size());

        for (int i = 0; i < results.size(); i++) {
            ByteBuffer buffer = getMessageResult.getMessageBufferList().get(i);
            MessageExt messageExt = MessageDecoder.decode(buffer);
            Assert.assertEquals(i, messageExt.getQueueOffset());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getMsgId(), messageExt.getMsgId());
            Assert.assertEquals(results.get(i).getAppendMessageResult().getWroteOffset(), messageExt.getCommitLogOffset());
        }
        messageStore.destroy();
        messageStore.shutdown();
    }


    @Test
    public void testCommittedPos() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore leaderStore = createDledgerMessageStore(createBaseDir(), group,"n0", peers, "n0", false, 0);

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner =  buildMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.OS_PAGECACHE_BUSY, putMessageResult.getPutMessageStatus());

        Thread.sleep(1000);

        Assert.assertEquals(0, leaderStore.getCommitLog().getMaxOffset());
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));


        DefaultMessageStore followerStore = createDledgerMessageStore(createBaseDir(), group,"n1", peers, "n0", false, 0);
        Thread.sleep(2000);

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(1, followerStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);


        leaderStore.destroy();
        followerStore.destroy();

        leaderStore.shutdown();
        followerStore.shutdown();
    }

    @Test
    public void testIPv6HostMsgCommittedPos() throws Exception {
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        String group = UUID.randomUUID().toString();
        DefaultMessageStore leaderStore = createDledgerMessageStore(createBaseDir(), group,"n0", peers, "n0", false, 0);

        String topic = UUID.randomUUID().toString();
        MessageExtBrokerInner msgInner =  buildIPv6HostMessage();
        msgInner.setTopic(topic);
        msgInner.setQueueId(0);
        PutMessageResult putMessageResult = leaderStore.putMessage(msgInner);
        Assert.assertEquals(PutMessageStatus.OS_PAGECACHE_BUSY, putMessageResult.getPutMessageStatus());

        Thread.sleep(1000);

        Assert.assertEquals(0, leaderStore.getCommitLog().getMaxOffset());
        Assert.assertEquals(0, leaderStore.getMaxOffsetInQueue(topic, 0));


        DefaultMessageStore followerStore = createDledgerMessageStore(createBaseDir(), group,"n1", peers, "n0", false, 0);
        Thread.sleep(2000);

        Assert.assertEquals(1, leaderStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertEquals(1, followerStore.getMaxOffsetInQueue(topic, 0));
        Assert.assertTrue(leaderStore.getCommitLog().getMaxOffset() > 0);


        leaderStore.destroy();
        followerStore.destroy();

        leaderStore.shutdown();
        followerStore.shutdown();
    }


}
