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
package org.apache.rocketmq.broker;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class PersistStatisticsTest {
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private int messageNum = 55;
    private final String StoreMessage = "Once, there was a chance for me!";

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
    }

    @Test
    public void testPersistAndLoad() throws Exception {
        //start broker
        BrokerController brokerController = new BrokerController(
            new BrokerConfig(),
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig());
        assertThat(brokerController.initialize());
        brokerController.start();
        //write message
        for (int i = 0; i < messageNum; ++i) {
            brokerController.getMessageStore().putMessage(buildMessage(StoreMessage.getBytes(), "FooBar"));
        }
        String statisticDataWrite = brokerController.getBrokerStats().encode();
        System.out.println(statisticDataWrite);
        brokerController.shutdown();
        //restart
        Thread.sleep(20000);
        brokerController = new BrokerController(
            new BrokerConfig(),
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig());
        assertThat(brokerController.initialize());
        brokerController.start();
        String statisticDataRead = brokerController.getBrokerStats().encode();
        assertEquals(statisticDataWrite, statisticDataRead);
    }

    private MessageExtBrokerInner buildMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        return msg;
    }
}
