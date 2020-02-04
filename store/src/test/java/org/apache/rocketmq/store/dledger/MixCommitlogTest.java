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

import java.util.UUID;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.StoreTestBase;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.junit.Assert;
import org.junit.Test;

public class MixCommitlogTest extends MessageStoreTestBase {



    @Test
    public void testFallBehindCQ() throws Exception {
        String base =  createBaseDir();
        String topic = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();
        {
            DefaultMessageStore originalStore = createMessageStore(base, false);
            doPutMessages(originalStore, topic, 0, 1000, 0);
            Assert.assertEquals(11, originalStore.getMaxPhyOffset()/originalStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
            Thread.sleep(500);
            Assert.assertEquals(0, originalStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, originalStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, originalStore.dispatchBehindBytes());
            doGetMessages(originalStore, topic, 0, 1000, 0);
            originalStore.shutdown();
        }
        //delete the cq files
        {
            StoreTestBase.deleteFile(StorePathConfigHelper.getStorePathConsumeQueue(base));
        }
        {
            DefaultMessageStore dledgerStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            Thread.sleep(2000);
            Assert.assertEquals(0, dledgerStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, dledgerStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, dledgerStore.dispatchBehindBytes());
            doGetMessages(dledgerStore, topic, 0, 1000, 0);
            doPutMessages(dledgerStore, topic, 0, 1000, 1000);
            Thread.sleep(500);
            Assert.assertEquals(0, dledgerStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, dledgerStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, dledgerStore.dispatchBehindBytes());
            doGetMessages(dledgerStore, topic, 0, 2000, 0);
            dledgerStore.shutdown();
        }
    }



    @Test
    public void testPutAndGet() throws Exception {
        String base =  createBaseDir();
        String topic = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();

        long dividedOffset;
        {
            DefaultMessageStore originalStore = createMessageStore(base, false);
            doPutMessages(originalStore, topic, 0, 1000, 0);
            Thread.sleep(500);
            Assert.assertEquals(0, originalStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, originalStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, originalStore.dispatchBehindBytes());
            dividedOffset = originalStore.getCommitLog().getMaxOffset();
            dividedOffset = dividedOffset - dividedOffset % originalStore.getMessageStoreConfig().getMappedFileSizeCommitLog() + originalStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
            doGetMessages(originalStore, topic, 0, 1000, 0);
            originalStore.shutdown();
        }
        {
            DefaultMessageStore recoverOriginalStore = createMessageStore(base, true);
            Thread.sleep(500);
            Assert.assertEquals(0, recoverOriginalStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, recoverOriginalStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, recoverOriginalStore.dispatchBehindBytes());
            doGetMessages(recoverOriginalStore, topic, 0, 1000, 0);
            recoverOriginalStore.shutdown();
        }
        {
            DefaultMessageStore dledgerStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) dledgerStore.getCommitLog();
            Assert.assertFalse(dLedgerCommitLog.getdLedgerServer().getdLedgerConfig().isEnableDiskForceClean());
            Assert.assertEquals(dividedOffset, dLedgerCommitLog.getDividedCommitlogOffset());
            Assert.assertEquals(0, dledgerStore.dispatchBehindBytes());
            Assert.assertEquals(dividedOffset, dLedgerCommitLog.getMaxOffset());
            Thread.sleep(2000);
            doPutMessages(dledgerStore, topic, 0, 1000, 1000);
            Thread.sleep(500);
            Assert.assertEquals(0, dledgerStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, dledgerStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, dledgerStore.dispatchBehindBytes());
            doGetMessages(dledgerStore, topic, 0, 2000, 0);
            dledgerStore.shutdown();
        }
        {
            DefaultMessageStore recoverDledgerStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) recoverDledgerStore.getCommitLog();
            Assert.assertFalse(dLedgerCommitLog.getdLedgerServer().getdLedgerConfig().isEnableDiskForceClean());
            Assert.assertEquals(dividedOffset, dLedgerCommitLog.getDividedCommitlogOffset());
            Thread.sleep(2000);
            doPutMessages(recoverDledgerStore, topic, 0, 1000, 2000);
            Thread.sleep(500);
            Assert.assertEquals(0, recoverDledgerStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(3000, recoverDledgerStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, recoverDledgerStore.dispatchBehindBytes());
            doGetMessages(recoverDledgerStore, topic, 0, 3000, 0);
            recoverDledgerStore.shutdown();
        }
    }

    @Test
    public void testDeleteExpiredFiles() throws Exception {
        String base =  createBaseDir();
        String topic = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();

        long dividedOffset;
        {
            DefaultMessageStore originalStore = createMessageStore(base, false);
            doPutMessages(originalStore, topic, 0, 1000, 0);
            Thread.sleep(500);
            Assert.assertEquals(0, originalStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, originalStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, originalStore.dispatchBehindBytes());
            dividedOffset = originalStore.getCommitLog().getMaxOffset();
            dividedOffset = dividedOffset - dividedOffset % originalStore.getMessageStoreConfig().getMappedFileSizeCommitLog() + originalStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
            originalStore.shutdown();
        }
        long maxPhysicalOffset;
        {
            DefaultMessageStore dledgerStore = createDledgerMessageStore(base, group, "n0", peers, null, true, 0);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) dledgerStore.getCommitLog();
            Assert.assertTrue(dledgerStore.getMessageStoreConfig().isCleanFileForciblyEnable());
            Assert.assertFalse(dLedgerCommitLog.getdLedgerServer().getdLedgerConfig().isEnableDiskForceClean());
            Assert.assertEquals(dividedOffset, dLedgerCommitLog.getDividedCommitlogOffset());
            Thread.sleep(2000);
            doPutMessages(dledgerStore, topic, 0, 1000, 1000);
            Thread.sleep(500);
            Assert.assertEquals(0, dledgerStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, dledgerStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, dledgerStore.dispatchBehindBytes());
            Assert.assertEquals(0, dledgerStore.getMinPhyOffset());
            maxPhysicalOffset = dledgerStore.getMaxPhyOffset();
            Assert.assertTrue(maxPhysicalOffset > 0);

            doGetMessages(dledgerStore, topic, 0, 2000, 0);

            for (int i = 0; i < 100; i++) {
                dledgerStore.getCommitLog().deleteExpiredFile(System.currentTimeMillis(), 0, 0, true);
            }
            Assert.assertEquals(dividedOffset, dledgerStore.getMinPhyOffset());
            Assert.assertEquals(maxPhysicalOffset, dledgerStore.getMaxPhyOffset());
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(Integer.MAX_VALUE, dledgerStore.getCommitLog().deleteExpiredFile(System.currentTimeMillis(), 0, 0, true));
            }
            Assert.assertEquals(dividedOffset, dledgerStore.getMinPhyOffset());
            Assert.assertEquals(maxPhysicalOffset, dledgerStore.getMaxPhyOffset());

            Assert.assertTrue(dledgerStore.getMessageStoreConfig().isCleanFileForciblyEnable());
            Assert.assertTrue(dLedgerCommitLog.getdLedgerServer().getdLedgerConfig().isEnableDiskForceClean());

            //Test fresh
            dledgerStore.getMessageStoreConfig().setCleanFileForciblyEnable(false);
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(Integer.MAX_VALUE, dledgerStore.getCommitLog().deleteExpiredFile(System.currentTimeMillis(), 0, 0, true));
            }
            Assert.assertFalse(dLedgerCommitLog.getdLedgerServer().getdLedgerConfig().isEnableDiskForceClean());
            doGetMessages(dledgerStore, topic, 0, 1000, 1000);
            dledgerStore.shutdown();
        }
    }
}
