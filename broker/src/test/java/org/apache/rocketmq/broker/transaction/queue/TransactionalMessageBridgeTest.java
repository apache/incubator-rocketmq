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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalMessageBridgeTest {

    private TransactionalMessageBridge transactionBridge;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Mock
    private MessageStore messageStore;

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        transactionBridge = new TransactionalMessageBridge(brokerController, messageStore);
    }

    @Test
    public void testPutOpMessage() {
        boolean isSuccess = transactionBridge.putOpMessage(createMessageBrokerInner(), TransactionalMessageUtil.REMOVETAG);
        assertThat(isSuccess).isTrue();
    }

    @Test
    public void testPutHalfMessage() {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult
            (PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        PutMessageResult result = transactionBridge.putHalfMessage(createMessageBrokerInner());
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
    }

    @Test
    public void testFetchMessageQueues() {
        Set<MessageQueue> messageQueues = transactionBridge.fetchMessageQueues(MixAll.RMQ_SYS_TRANS_HALF_TOPIC);
        assertThat(messageQueues.size()).isEqualTo(1);
    }

    @Test
    public void testFetchConsumeOffset() {
        MessageQueue mq = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), this.brokerController.getBrokerConfig().getBrokerName(),
            0);
        long offset = transactionBridge.fetchConsumeOffset(mq);
        assertThat(offset).isGreaterThan(-1);
    }

    @Test
    public void updateConsumeOffset() {
        MessageQueue mq = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), this.brokerController.getBrokerConfig().getBrokerName(),
            0);
        transactionBridge.updateConsumeOffset(mq, 0);
    }

    @Test
    public void testGetHalfMessage() {
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(),  ArgumentMatchers.nullable(MessageFilter.class))).thenReturn(createGetMessageResult(GetMessageStatus.NO_MESSAGE_IN_QUEUE));
        PullResult result = transactionBridge.getHalfMessage(0, 0, 1);
        assertThat(result.getPullStatus()).isEqualTo(PullStatus.NO_NEW_MSG);
    }

    @Test
    public void testGetOpMessage() {
        when(messageStore.getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(),  ArgumentMatchers.nullable(MessageFilter.class))).thenReturn(createGetMessageResult(GetMessageStatus.NO_MESSAGE_IN_QUEUE));
        PullResult result = transactionBridge.getOpMessage(0, 0, 1);
        assertThat(result.getPullStatus()).isEqualTo(PullStatus.NO_NEW_MSG);
    }

    @Test
    public void testPutMessageReturnResult() {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult
            (PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        PutMessageResult result = transactionBridge.putMessageReturnResult(createMessageBrokerInner());
        assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
    }

    @Test
    public void testPutMessage() {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult
            (PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        Boolean success = transactionBridge.putMessage(createMessageBrokerInner());
        assertThat(success).isEqualTo(true);
    }

    @Test
    public void testRenewImmunityHalfMessageInner() {
        MessageExt messageExt = createMessageBrokerInner();
        final String offset = "123456789";
        MessageExtBrokerInner msgInner = transactionBridge.renewImmunityHalfMessageInner(messageExt);
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,offset);
        assertThat(msgInner).isNotNull();
        Map<String,String> properties = msgInner.getProperties();
        assertThat(properties).isNotNull();
        String resOffset = properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        assertThat(resOffset).isEqualTo(offset);
    }


    @Test
    public void testRenewHalfMessageInner() {
        MessageExt messageExt = new MessageExt();
        long bornTimeStamp = messageExt.getBornTimestamp();
        MessageExt messageExtRes = transactionBridge.renewHalfMessageInner(messageExt);
        assertThat( messageExtRes.getBornTimestamp()).isEqualTo(bornTimeStamp);
    }

    @Test
    public void testLookMessageByOffset(){
        when(messageStore.lookMessageByOffset(anyLong())).thenReturn(new MessageExt());
        MessageExt messageExt = transactionBridge.lookMessageByOffset(123);
        assertThat(messageExt).isNotNull();
    }

    @Test
    public void testGetHalfMessageStatusFound() {
        when(messageStore
                .getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), ArgumentMatchers.nullable(MessageFilter.class)))
                .thenReturn(createGetMessageResult(GetMessageStatus.FOUND));
        PullResult result = transactionBridge.getHalfMessage(0, 0, 1);
        assertThat(result.getPullStatus()).isEqualTo(PullStatus.FOUND);
    }

    @Test
    public void testGetHalfMessageNull() {
        when(messageStore
                .getMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(), ArgumentMatchers.nullable(MessageFilter.class)))
                .thenReturn(null);
        PullResult result = transactionBridge.getHalfMessage(0, 0, 1);
        assertThat(result).isNull();
    }

    private GetMessageResult createGetMessageResult(GetMessageStatus status) {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(status);
        getMessageResult.setMinOffset(100);
        getMessageResult.setMaxOffset(1024);
        getMessageResult.setNextBeginOffset(516);
        return getMessageResult;
    }

    private MessageExtBrokerInner createMessageBrokerInner() {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTransactionId("12342123444");
        inner.setBornTimestamp(System.currentTimeMillis());
        inner.setBody("prepare".getBytes());
        inner.setMsgId("123456-123");
        inner.setQueueId(0);
        inner.setTopic("hello");
        return inner;
    }
}
