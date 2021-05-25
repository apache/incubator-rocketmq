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

import java.util.Collections;
import java.util.HashSet;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RemoteBrokerOffsetStoreTest {
    @Mock
    private MQClientInstance mQClientFactory;
    @Mock
    private MQClientAPIImpl mqClientAPI;
    private String group = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "DefaultBrokerName";

    @Before
    public void init() {
        System.setProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("java.io.tmpdir") + ".rocketmq_offsets");
        String clientId = new ClientConfig().buildMQClientId() + "#TestNamespace" + System.currentTimeMillis();
        when(mQClientFactory.getClientId()).thenReturn(clientId);
        when(mQClientFactory.findBrokerAddressInAdmin(brokerName)).thenReturn(new FindBrokerResult("127.0.0.1", false));
        when(mQClientFactory.getMQClientAPIImpl()).thenReturn(mqClientAPI);
    }

    @Test
    public void testUpdateOffset() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 1);

        offsetStore.updateOffset(messageQueue, 1024, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1024);

        offsetStore.updateOffset(messageQueue, 1023, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1022, true);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1023);
    }

    @Test
    public void testReadOffset_WithException() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 2);

        offsetStore.updateOffset(messageQueue, 1024, false);

        doThrow(new MQBrokerException(-1, "", null))
            .when(mqClientAPI).queryConsumerOffset(anyString(), any(QueryConsumerOffsetRequestHeader.class), anyLong());
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(-1);

      doThrow(new InterruptedException(""))
          .when(mqClientAPI).queryConsumerOffset(anyString(), any(QueryConsumerOffsetRequestHeader.class), anyLong());
      assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(-2);

        doThrow(new RemotingException("", null))
            .when(mqClientAPI).queryConsumerOffset(anyString(), any(QueryConsumerOffsetRequestHeader.class), anyLong());
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(-3);
    }

    @Test
    public void testReadOffset_Success() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        final MessageQueue messageQueue = new MessageQueue(topic, brokerName, 3);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                UpdateConsumerOffsetRequestHeader updateRequestHeader = mock.getArgument(1);
                when(mqClientAPI.queryConsumerOffset(anyString(), any(QueryConsumerOffsetRequestHeader.class), anyLong())).thenReturn(updateRequestHeader.getCommitOffset());
                return null;
            }
        }).when(mqClientAPI).updateConsumerOffsetOneway(any(String.class), any(UpdateConsumerOffsetRequestHeader.class), any(Long.class));

        offsetStore.updateOffset(messageQueue, 1024, false);
        offsetStore.persist(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1024);

        offsetStore.updateOffset(messageQueue, 1023, false);
        offsetStore.persist(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1022, true);
        offsetStore.persist(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1023);

        offsetStore.updateOffset(messageQueue, 1025, false);
        offsetStore.persistAll(new HashSet<MessageQueue>(Collections.singletonList(messageQueue)));
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE)).isEqualTo(1025);
    }

    @Test
    public void testRemoveOffset() throws Exception {
        OffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, group);
        final MessageQueue messageQueue = new MessageQueue(topic, brokerName, 4);

        offsetStore.updateOffset(messageQueue, 1024, false);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(1024);

        offsetStore.removeOffset(messageQueue);
        assertThat(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY)).isEqualTo(-1);
    }
}