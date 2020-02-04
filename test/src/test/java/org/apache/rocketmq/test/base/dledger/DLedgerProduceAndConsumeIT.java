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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.test.base.dledger;

import java.util.UUID;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.rocketmq.test.base.IntegrationTestBase.nextPort;
import static sun.util.locale.BaseLocale.SEP;

public class DLedgerProduceAndConsumeIT {

    public BrokerConfig buildBrokerConfig(String cluster, String brokerName) {
        BrokerConfig brokerConfig =  new BrokerConfig();
        brokerConfig.setBrokerClusterName(cluster);
        brokerConfig.setBrokerName(brokerName);
        brokerConfig.setBrokerIP1("127.0.0.1");
        brokerConfig.setNamesrvAddr(BaseConf.nsAddr);
        return brokerConfig;
    }

    public MessageStoreConfig buildStoreConfig(String brokerName, String peers, String selfId) {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        String baseDir =  IntegrationTestBase.createBaseDir();
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + SEP + "commitlog");
        storeConfig.setHaListenPort(nextPort());
        storeConfig.setMappedFileSizeCommitLog(10 * 1024 * 1024);
        storeConfig.setEnableDLegerCommitLog(true);
        storeConfig.setdLegerGroup(brokerName);
        storeConfig.setdLegerSelfId(selfId);
        storeConfig.setdLegerPeers(peers);
        return storeConfig;
    }

    @Test
    public void testProduceAndConsume() throws Exception {
        String cluster = UUID.randomUUID().toString();
        String brokerName = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d", nextPort());
        BrokerConfig brokerConfig = buildBrokerConfig(cluster, brokerName);
        MessageStoreConfig storeConfig = buildStoreConfig(brokerName, peers, selfId);
        BrokerController brokerController = IntegrationTestBase.createAndStartBroker(storeConfig, brokerConfig);
        Thread.sleep(3000);

        Assert.assertEquals(BrokerRole.SYNC_MASTER, storeConfig.getBrokerRole());


        String topic = UUID.randomUUID().toString();
        String consumerGroup = UUID.randomUUID().toString();
        IntegrationTestBase.initTopic(topic, BaseConf.nsAddr, cluster, 1);
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(BaseConf.nsAddr);
        DefaultMQPullConsumer consumer = ConsumerFactory.getRMQPullConsumer(BaseConf.nsAddr, consumerGroup);

        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic(topic);
            message.setBody(("Hello" + i).getBytes());
            SendResult sendResult = producer.send(message);
            Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
            Assert.assertEquals(0, sendResult.getMessageQueue().getQueueId());
            Assert.assertEquals(brokerName, sendResult.getMessageQueue().getBrokerName());
            Assert.assertEquals(i, sendResult.getQueueOffset());
            Assert.assertNotNull(sendResult.getMsgId());
            Assert.assertNotNull(sendResult.getOffsetMsgId());
        }

        Thread.sleep(500);
        Assert.assertEquals(0, brokerController.getMessageStore().getMinOffsetInQueue(topic, 0));
        Assert.assertEquals(10, brokerController.getMessageStore().getMaxOffsetInQueue(topic, 0));

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        PullResult pullResult= consumer.pull(messageQueue, "*", 0, 32);
        Assert.assertEquals(PullStatus.FOUND, pullResult.getPullStatus());
        Assert.assertEquals(10, pullResult.getMsgFoundList().size());

        for (int i = 0; i < 10; i++) {
            MessageExt messageExt = pullResult.getMsgFoundList().get(i);
            Assert.assertEquals(i, messageExt.getQueueOffset());
            Assert.assertArrayEquals(("Hello" + i).getBytes(), messageExt.getBody());
        }

        producer.shutdown();
        consumer.shutdown();
        brokerController.shutdown();
    }
}
