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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.domain.LogicalQueuesInfoInBroker;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SendMessageProcessorTest {
    private SendMessageProcessor sendMessageProcessor;
    @Mock
    private ChannelHandlerContext handlerContext;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private MessageStore messageStore;

    @Mock
    private TransactionalMessageService transactionMsgService;

    private String topic = "FooBar";
    private String group = "FooBarGroup";

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        when(messageStore.now()).thenReturn(System.currentTimeMillis());
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(handlerContext.channel()).thenReturn(mockChannel);
        when(messageStore.lookMessageByOffset(anyLong())).thenReturn(new MessageExt());
        sendMessageProcessor = new SendMessageProcessor(brokerController);

        brokerController.getTopicConfigManager().updateTopicConfig(new TopicConfig(topic, 8, 8, PermName.PERM_WRITE|PermName.PERM_READ));
    }

    @Test
    public void testProcessRequest() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));
        assertPutResult(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_WithHook() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));
        List<SendMessageHook> sendMessageHookList = new ArrayList<>();
        final SendMessageContext[] sendMessageContext = new SendMessageContext[1];
        SendMessageHook sendMessageHook = new SendMessageHook() {
            @Override
            public String hookName() {
                return null;
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendMessageContext[0] = context;
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        };
        sendMessageHookList.add(sendMessageHook);
        sendMessageProcessor.registerSendMessageHook(sendMessageHookList);
        assertPutResult(ResponseCode.SUCCESS);
        System.out.println(sendMessageContext[0]);
        assertThat(sendMessageContext[0]).isNotNull();
        assertThat(sendMessageContext[0].getTopic()).isEqualTo(topic);
        assertThat(sendMessageContext[0].getProducerGroup()).isEqualTo(group);
    }

    @Test
    public void testProcessRequest_FlushTimeOut() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.FLUSH_DISK_TIMEOUT);
    }

    @Test
    public void testProcessRequest_MessageIllegal() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.MESSAGE_ILLEGAL);
    }

    @Test
    public void testProcessRequest_CreateMappedFileFailed() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testProcessRequest_FlushSlaveTimeout() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.FLUSH_SLAVE_TIMEOUT);
    }

    @Test
    public void testProcessRequest_PageCacheBusy() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testProcessRequest_PropertiesTooLong() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.MESSAGE_ILLEGAL);
    }

    @Test
    public void testProcessRequest_ServiceNotAvailable() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.SERVICE_NOT_AVAILABLE);
    }

    @Test
    public void testProcessRequest_SlaveNotAvailable() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.SLAVE_NOT_AVAILABLE, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR))));
        assertPutResult(ResponseCode.SLAVE_NOT_AVAILABLE);
    }

    @Test
    public void testProcessRequest_WithMsgBack() throws RemotingCommandException {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));
        final RemotingCommand request = createSendMsgBackCommand(RequestCode.CONSUMER_SEND_MSG_BACK);

        sendMessageProcessor = new SendMessageProcessor(brokerController);
        final RemotingCommand response = sendMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_Transaction() throws RemotingCommandException {
        brokerController.setTransactionalMessageService(transactionMsgService);
        when(brokerController.getTransactionalMessageService().asyncPrepareMessage(any(MessageExtBrokerInner.class)))
                .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));
        RemotingCommand request = createSendTransactionMsgCommand(RequestCode.SEND_MESSAGE);
        final RemotingCommand[] response = new RemotingCommand[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                response[0] = invocation.getArgument(0);
                return null;
            }
        }).when(handlerContext).writeAndFlush(any(Object.class));
        RemotingCommand responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
        if (responseToReturn != null) {
            assertThat(response[0]).isNull();
            response[0] = responseToReturn;
        }
        assertThat(response[0].getCode()).isEqualTo(ResponseCode.SUCCESS);

    }

    @Test
    public void testProcessRequest_LogicalQueue() throws Exception {
        when(messageStore.asyncPutMessage(any(MessageExtBrokerInner.class)))
            .thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));

        LogicalQueuesInfoInBroker logicalQueuesInfo = brokerController.getTopicConfigManager().getOrCreateLogicalQueuesInfo(topic);
        LogicalQueueRouteData queueRouteData1 = new LogicalQueueRouteData(0, 0, new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), 1), MessageQueueRouteState.Normal, 0, -1, -1, -1, brokerController.getBrokerAddr());
        logicalQueuesInfo.put(0, Lists.newArrayList(queueRouteData1));
        logicalQueuesInfo.updateQueueRouteDataByQueueId(queueRouteData1.getQueueId(), queueRouteData1);

        SendMessageRequestHeader requestHeader = createSendMsgRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();

        // normal
        RemotingCommand responseToReturn;
        {
            CompletableFuture<RemotingCommand> responseFuture = new CompletableFuture<>();
            doAnswer(invocation -> {
                responseFuture.complete(invocation.getArgument(0));
                return null;
            }).when(handlerContext).writeAndFlush(any(Object.class));

            responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
            if (responseToReturn == null) {
                responseToReturn = responseFuture.get(3, TimeUnit.SECONDS);
            }
        }

        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getOpaque()).isEqualTo(request.getOpaque());

        // read only
        queueRouteData1.setState(MessageQueueRouteState.ReadOnly);
        responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(responseToReturn.getRemark()).contains("not writable");

        // read only and forward
        logicalQueuesInfo.get(0).add(new LogicalQueueRouteData(0, 100, new MessageQueue(topic, "broker2", 1), MessageQueueRouteState.Normal, 0, -1, -1, -1, brokerController.getBrokerAddr()));

        responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(responseToReturn.getRemark()).contains("forward error");

        // read only and redirect
        requestHeader = (SendMessageRequestHeader) request.readCustomHeader();
        requestHeader.setSysFlag(MessageSysFlag.LOGICAL_QUEUE_FLAG);
        request.makeCustomHeaderToNet();
        responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(responseToReturn.getExtFields()).containsKey(MessageConst.PROPERTY_REDIRECT);
    }

    private RemotingCommand createSendTransactionMsgCommand(int requestCode) {
        SendMessageRequestHeader header = createSendMsgRequestHeader();
        int sysFlag = header.getSysFlag();
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(header.getProperties());
        oriProps.put(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        header.setProperties(MessageDecoder.messageProperties2String(oriProps));
        sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        header.setSysFlag(sysFlag);
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, header);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();
        return request;
    }

    private SendMessageRequestHeader createSendMsgRequestHeader() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(3);
        requestHeader.setQueueId(1);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(124);
        requestHeader.setReconsumeTimes(0);
        return requestHeader;
    }

    private RemotingCommand createSendMsgCommand(int requestCode) {
        SendMessageRequestHeader requestHeader = createSendMsgRequestHeader();

        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand createSendMsgBackCommand(int requestCode) {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();

        requestHeader.setMaxReconsumeTimes(3);
        requestHeader.setDelayLevel(4);
        requestHeader.setGroup(group);
        requestHeader.setOffset(123L);

        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private void assertPutResult(int responseCode) throws RemotingCommandException {
        final RemotingCommand request = createSendMsgCommand(RequestCode.SEND_MESSAGE);
        final RemotingCommand[] response = new RemotingCommand[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                response[0] = invocation.getArgument(0);
                return null;
            }
        }).when(handlerContext).writeAndFlush(any(Object.class));
        RemotingCommand responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
        if (responseToReturn != null) {
            assertThat(response[0]).isNull();
            response[0] = responseToReturn;
        }
        assertThat(response[0].getCode()).isEqualTo(responseCode);
        assertThat(response[0].getOpaque()).isEqualTo(request.getOpaque());
    }
}