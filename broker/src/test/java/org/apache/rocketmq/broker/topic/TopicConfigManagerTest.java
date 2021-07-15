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

package org.apache.rocketmq.broker.topic;

import com.google.common.collect.Lists;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.domain.LogicalQueuesInfoInBroker;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicConfigManagerTest {
    @Mock
    private DefaultMessageStore messageStore;
    @Mock
    private BrokerController brokerController;

    private TopicConfigManager topicConfigManager;

    private static final String topic = "FooBar";
    private static final String broker1Name = "broker1";
    private static final String broker1Addr = "127.0.0.1:12345";
    private static final int queueId1 = 1;
    private static final String broker2Name = "broker2";
    private static final String broker2Addr = "127.0.0.2:12345";
    private static final int queueId2 = 2;

    @Before
    public void before() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(broker1Name);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);

        when(brokerController.getMessageStore()).thenReturn(messageStore);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(System.getProperty("java.io.tmpdir"));
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        this.topicConfigManager = new TopicConfigManager(brokerController);
        this.topicConfigManager.getTopicConfigTable().put(topic, new TopicConfig(topic));
    }

    @After
    public void after() throws Exception {
        if (topicConfigManager != null) {
            Files.deleteIfExists(Paths.get(topicConfigManager.configFilePath()));
        }
    }

    @Test
    public void logicalQueueCleanTest() {
        LogicalQueuesInfoInBroker info = this.topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        topicConfigManager.logicalQueueClean();
        assertThat(info).isEmpty();

        final int logicalQueueIndex = 0;
        LogicalQueueRouteData queueRouteData1 = new LogicalQueueRouteData(logicalQueueIndex, 0, new MessageQueue(topic, broker1Name, queueId1), MessageQueueRouteState.Normal, 0, -1, -1, -1, broker1Addr);
        List<LogicalQueueRouteData> l = Lists.newArrayList(new LogicalQueueRouteData(queueRouteData1));
        info.put(logicalQueueIndex, l);

        topicConfigManager.logicalQueueClean();
        assertThat(info.get(logicalQueueIndex)).isEqualTo(Collections.singletonList(queueRouteData1));
        verify(messageStore, never()).getCommitLogOffsetInQueue(eq(topic), eq(queueId1), anyLong());
        verify(messageStore, never()).getMinPhyOffset();
        verify(brokerController, never()).registerIncrementBrokerData(ArgumentMatchers.<TopicConfig>argThat(arg -> topic.equals(arg.getTopicName())), any(DataVersion.class));

        LogicalQueueRouteData queueRouteData2 = new LogicalQueueRouteData(logicalQueueIndex, 100, new MessageQueue(topic, broker2Name, queueId2), MessageQueueRouteState.Normal, 0, -1, -1, -1, broker2Addr);
        l.add(new LogicalQueueRouteData(queueRouteData2));
        queueRouteData1 = l.get(0);
        queueRouteData1.setState(MessageQueueRouteState.ReadOnly);
        queueRouteData1.setOffsetMax(100);
        queueRouteData1.setFirstMsgTimeMillis(200);
        queueRouteData1.setLastMsgTimeMillis(300);
        queueRouteData1 = new LogicalQueueRouteData(queueRouteData1);
        LogicalQueueRouteData queueRouteData3 = new LogicalQueueRouteData(logicalQueueIndex, 200, new MessageQueue(topic, broker1Name, queueId1), MessageQueueRouteState.Normal, 100, -1, -1, -1, broker1Addr);
        l.add(new LogicalQueueRouteData(queueRouteData3));
        queueRouteData2 = l.get(1);
        queueRouteData2.setState(MessageQueueRouteState.ReadOnly);
        queueRouteData2.setOffsetMax(100);
        queueRouteData2.setFirstMsgTimeMillis(400);
        queueRouteData2.setLastMsgTimeMillis(500);
        queueRouteData2 = new LogicalQueueRouteData(queueRouteData2);
        when(messageStore.getCommitLogOffsetInQueue(eq(topic), eq(queueId1), eq(queueRouteData1.getOffsetMax() - 1))).thenReturn(1000L);
        when(messageStore.getMinPhyOffset()).thenReturn(0L);
        topicConfigManager.logicalQueueClean();
        assertThat(info.get(logicalQueueIndex)).isEqualTo(Arrays.asList(queueRouteData1, queueRouteData2, queueRouteData3));
        verify(messageStore).getCommitLogOffsetInQueue(eq(topic), eq(queueId1), eq(queueRouteData1.getOffsetMax() - 1));
        verify(messageStore).getMinPhyOffset();
        verify(brokerController, never()).registerIncrementBrokerData(ArgumentMatchers.<TopicConfig>argThat(arg -> topic.equals(arg.getTopicName())), any(DataVersion.class));

        when(messageStore.getMinPhyOffset()).thenReturn(2000L);
        topicConfigManager.logicalQueueClean();
        assertThat(info.get(logicalQueueIndex)).isEqualTo(Collections.singletonList(queueRouteData3));
        verify(brokerController).registerIncrementBrokerData(ArgumentMatchers.<TopicConfig>argThat(arg -> topic.equals(arg.getTopicName())), any(DataVersion.class));
    }
}