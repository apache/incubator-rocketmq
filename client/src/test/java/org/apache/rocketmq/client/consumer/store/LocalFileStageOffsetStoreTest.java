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
package org.apache.rocketmq.client.consumer.store;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LocalFileStageOffsetStoreTest {
    @Mock
    private MQClientInstance mQClientFactory;
    private String group = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "DefaultBrokerName";

    @Before
    public void init() {
        System.setProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("java.io.tmpdir") + File.separator + ".rocketmq_offsets");
        String clientId = new ClientConfig().buildMQClientId() + "#TestNamespace" + System.currentTimeMillis();
        when(mQClientFactory.getClientId()).thenReturn(clientId);
    }

    @Test
    public void testUpdateStageOffset() throws Exception {
        StageOffsetStore offsetStore = new LocalFileStageOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 1);
        offsetStore.updateStageOffset(messageQueue, "strategyId", "groupId", 1024, false);

        assertThat(offsetStore.readStageOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY).get("strategyId").get("groupId")).isEqualTo(1024);

        offsetStore.updateStageOffset(messageQueue, "strategyId", "groupId", 1023, false);
        assertThat(offsetStore.readStageOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY).get("strategyId").get("groupId")).isEqualTo(1023);

        offsetStore.updateStageOffset(messageQueue, "strategyId", "groupId", 1022, true);
        assertThat(offsetStore.readStageOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY).get("strategyId").get("groupId")).isEqualTo(1023);
    }

    @Test
    public void testReadStageOffset_FromStore() throws Exception {
        StageOffsetStore offsetStore = new LocalFileStageOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 2);

        offsetStore.updateStageOffset(messageQueue, "strategyId", "groupId", 1024, false);
        assertThat(offsetStore.readStageOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(new HashMap<>());

        offsetStore.persistAll(new HashSet<MessageQueue>(Collections.singletonList(messageQueue)));
        assertThat(offsetStore.readStageOffset(messageQueue, ReadOffsetType.READ_FROM_STORE).get("strategyId").get("groupId")).isEqualTo(1024);
    }

    @Test
    public void testCloneStageOffset() throws Exception {
        StageOffsetStore offsetStore = new LocalFileStageOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 3);
        offsetStore.updateStageOffset(messageQueue, "strategyId", "groupId", 1024, false);
        Map<MessageQueue, Map<String, Map<String, Integer>>> cloneOffsetTable = offsetStore.cloneStageOffsetTable(topic);

        assertThat(cloneOffsetTable.size()).isEqualTo(1);
        assertThat(cloneOffsetTable.get(messageQueue).get("strategyId").get("groupId")).isEqualTo(1024);
    }
}