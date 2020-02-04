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

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import java.io.File;
import java.net.UnknownHostException;
import java.util.Arrays;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.StoreTestBase;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Assert;

public class MessageStoreTestBase extends StoreTestBase {

    protected DefaultMessageStore createDledgerMessageStore(String base, String group, String selfId, String peers, String leaderId, boolean createAbort, int deleteFileNum) throws Exception {
        System.setProperty("dledger.disk.ratio.check", "0.95");
        System.setProperty("dledger.disk.ratio.clean", "0.95");
        baseDirs.add(base);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setMappedFileSizeCommitLog(1024 * 100);
        storeConfig.setMappedFileSizeConsumeQueue(1024);
        storeConfig.setMaxHashSlotNum(100);
        storeConfig.setMaxIndexNum(100 * 10);
        storeConfig.setStorePathRootDir(base);
        storeConfig.setStorePathCommitLog(base + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);

        storeConfig.setEnableDLegerCommitLog(true);
        storeConfig.setdLegerGroup(group);
        storeConfig.setdLegerPeers(peers);
        storeConfig.setdLegerSelfId(selfId);
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(storeConfig,  new BrokerStatsManager("DLedgerCommitlogTest"), (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {

        }, new BrokerConfig());
        DLedgerServer dLegerServer = ((DLedgerCommitLog) defaultMessageStore.getCommitLog()).getdLedgerServer();
        if (leaderId != null) {
            dLegerServer.getdLedgerConfig().setEnableLeaderElector(false);
            if (selfId.equals(leaderId)) {
                dLegerServer.getMemberState().changeToLeader(-1);
            } else {
                dLegerServer.getMemberState().changeToFollower(-1, leaderId);
            }

        }
        if (createAbort) {
            String fileName = StorePathConfigHelper.getAbortFile(storeConfig.getStorePathRootDir());
            makeSureFileExists(fileName);
        }
        if (deleteFileNum > 0) {
            DLedgerConfig config = dLegerServer.getdLedgerConfig();
            if (deleteFileNum > 0) {
                File dir = new File(config.getDataStorePath());
                File[] files = dir.listFiles();
                if (files != null) {
                    Arrays.sort(files);
                    for (int i = files.length - 1; i >= 0; i--) {
                        File file = files[i];
                        file.delete();
                        if (files.length - i >= deleteFileNum) {
                            break;
                        }
                    }
                }
            }
        }
        Assert.assertTrue(defaultMessageStore.load());
        defaultMessageStore.start();
        return defaultMessageStore;
    }


    protected DefaultMessageStore createMessageStore(String base, boolean createAbort) throws Exception {
        baseDirs.add(base);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setMappedFileSizeCommitLog(1024 * 100);
        storeConfig.setMappedFileSizeConsumeQueue(1024);
        storeConfig.setMaxHashSlotNum(100);
        storeConfig.setMaxIndexNum(100 * 10);
        storeConfig.setStorePathRootDir(base);
        storeConfig.setStorePathCommitLog(base + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore(storeConfig,  new BrokerStatsManager("CommitlogTest"), (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {

        }, new BrokerConfig());

        if (createAbort) {
            String fileName = StorePathConfigHelper.getAbortFile(storeConfig.getStorePathRootDir());
            makeSureFileExists(fileName);
        }
        Assert.assertTrue(defaultMessageStore.load());
        defaultMessageStore.start();
        return defaultMessageStore;
    }

    protected void doPutMessages(MessageStore messageStore, String topic, int queueId, int num, long beginLogicsOffset) throws UnknownHostException {
        for (int i = 0; i < num; i++) {
            MessageExtBrokerInner msgInner = buildMessage();
            msgInner.setTopic(topic);
            msgInner.setQueueId(queueId);
            PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
            Assert.assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            Assert.assertEquals(beginLogicsOffset + i, putMessageResult.getAppendMessageResult().getLogicsOffset());
        }
    }

    protected void doGetMessages(MessageStore messageStore, String topic, int queueId, int num, long beginLogicsOffset) {
        for (int i = 0; i < num; i++) {
            GetMessageResult getMessageResult =  messageStore.getMessage("group", topic, queueId, beginLogicsOffset + i, 3, null);
            Assert.assertNotNull(getMessageResult);
            Assert.assertTrue(!getMessageResult.getMessageBufferList().isEmpty());
            MessageExt messageExt = MessageDecoder.decode(getMessageResult.getMessageBufferList().get(0));
            Assert.assertEquals(beginLogicsOffset + i, messageExt.getQueueOffset());
            getMessageResult.release();
        }
    }

}
