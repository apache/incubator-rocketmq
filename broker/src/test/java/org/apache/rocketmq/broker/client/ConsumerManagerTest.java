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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerTest {
    private ConsumerManager consumerManager;
    private String group = "FooBar";
    private String clientId = "ClientId";
    private ClientChannelInfo clientInfo;

    @Mock
    private Channel channel;
    @Mock
    private ConsumerIdsChangeListener consumerIdsChangeListener;

    @Before
    public void init() {
        consumerManager = new ConsumerManager(consumerIdsChangeListener);
        clientInfo = new ClientChannelInfo(channel, clientId, LanguageCode.JAVA, 0);
    }

    @Test
    public void testFindChannel() {
        consumerManager.registerConsumer(group, clientInfo, ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING,
                ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, Collections.emptySet(), false);
        Channel c = consumerManager.findChannel(group, clientId).getChannel();
        assertThat(c).isSameAs(channel);
    }
}

