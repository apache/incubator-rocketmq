/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * HATest
 *
 */
public class HATest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;

    private MessageStore messageStore;
    private MessageStore slaveMessageStore;
    private MessageStoreConfig masterMessageStoreConfig;
    private MessageStoreConfig slaveStoreConfig;
    private BrokerStatsManager brokerStatsManager = new BrokerStatsManager("simpleTest");
    private String storePathRootParentDir = System.getProperty("user.home") + File.separator +
            UUID.randomUUID().toString().replace("-", "");
    private String storePathRootDir = storePathRootParentDir + File.separator + "store";
    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        masterMessageStoreConfig = new MessageStoreConfig();
        masterMessageStoreConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        masterMessageStoreConfig.setStorePathRootDir(storePathRootDir+File.separator+"master");
        masterMessageStoreConfig.setStorePathCommitLog(storePathRootDir+File.separator+"master"+ File.separator+"commitlog");
        buildMessageStoreConfig(masterMessageStoreConfig);
        slaveStoreConfig = new MessageStoreConfig();
        slaveStoreConfig.setBrokerRole(BrokerRole.SLAVE);
        slaveStoreConfig.setStorePathRootDir(storePathRootDir+File.separator+"slave");
        slaveStoreConfig.setStorePathCommitLog(storePathRootDir+File.separator+"slave"+ File.separator+"commitlog");
        slaveStoreConfig.setHaListenPort(10943);
        buildMessageStoreConfig(slaveStoreConfig);
        messageStore = buildMessageStore(masterMessageStoreConfig,0L);
        slaveMessageStore = buildMessageStore(slaveStoreConfig,1L);
        boolean load = messageStore.load();
        boolean slaveLoad = slaveMessageStore.load();
        slaveMessageStore.updateHaMasterAddress("127.0.0.1:10912");
        assertTrue(load);
        assertTrue(slaveLoad);
        messageStore.start();
        slaveMessageStore.start();
        Thread.sleep(6000L);//because the haClient will wait 5s after the first connectMaster failed,sleep 6s
    }

    @Test
    public void testHandleHA() {
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        for (long i = 0; i < totalMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (int i = 0; i < 100 && isCommitLogAvailable((DefaultMessageStore) messageStore); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        for (int i = 0; i < 100 && isCommitLogAvailable((DefaultMessageStore) slaveMessageStore); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = slaveMessageStore.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            assertTrue(GetMessageStatus.FOUND.equals(result.getStatus()));
            result.release();
        }
    }

    @After
    public void destroy() throws Exception{
        Thread.sleep(5000L);
        slaveMessageStore.shutdown();
        slaveMessageStore.destroy();
        messageStore.shutdown();
        messageStore.destroy();
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore(MessageStoreConfig messageStoreConfig,long brokerId) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        return new DefaultMessageStore(messageStoreConfig, brokerStatsManager, null, brokerConfig);
    }

    private void buildMessageStoreConfig(MessageStoreConfig messageStoreConfig){
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }

    private boolean isCommitLogAvailable(DefaultMessageStore store)  {
        try {

            Field serviceField = store.getClass().getDeclaredField("reputMessageService");
            serviceField.setAccessible(true);
            DefaultMessageStore.ReputMessageService reputService =
                    (DefaultMessageStore.ReputMessageService) serviceField.get(store);

            Method method = DefaultMessageStore.ReputMessageService.class.getDeclaredMethod("isCommitLogAvailable");
            method.setAccessible(true);
            return (boolean) method.invoke(reputService);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException |  NoSuchFieldException e ) {
            throw new RuntimeException(e);
        }
    }

}
