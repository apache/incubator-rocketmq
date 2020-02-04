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
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplyMessageProcessorTest {
    private ReplyMessageProcessor replyMessageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private MessageStore messageStore;
    @Mock
    private Channel channel;

    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ClientChannelInfo clientInfo;
    @Mock
    private Broker2Client broker2Client;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        clientInfo = new ClientChannelInfo(channel, "127.0.0.1", LanguageCode.JAVA, 0);
        brokerController.setMessageStore(messageStore);
        Field field = BrokerController.class.getDeclaredField("broker2Client");
        field.setAccessible(true);
        field.set(brokerController, broker2Client);
        when(messageStore.now()).thenReturn(System.currentTimeMillis());
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(handlerContext.channel()).thenReturn(mockChannel);
        replyMessageProcessor = new ReplyMessageProcessor(brokerController);
    }

    @Test
    public void testProcessRequest_Success() throws RemotingCommandException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        brokerController.getProducerManager().registerProducer(group, clientInfo);
        final RemotingCommand request = createSendMessageRequestHeaderCommand(RequestCode.SEND_REPLY_MESSAGE);
        when(brokerController.getBroker2Client().callClient(any(Channel.class), any(RemotingCommand.class))).thenReturn(createResponse(ResponseCode.SUCCESS, request));
        RemotingCommand responseToReturn = replyMessageProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getOpaque()).isEqualTo(request.getOpaque());
    }

    private RemotingCommand createSendMessageRequestHeaderCommand(int requestCode) {
        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();
        return request;
    }

    private SendMessageRequestHeader createSendMessageRequestHeader() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopic(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(3);
        requestHeader.setQueueId(1);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(124);
        requestHeader.setReconsumeTimes(0);
        Map<String, String> map = new HashMap<String, String>();
        map.put(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "127.0.0.1");
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));
        return requestHeader;
    }

    private RemotingCommand createResponse(int code, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(code);
        response.setOpaque(request.getOpaque());
        return response;
    }
}