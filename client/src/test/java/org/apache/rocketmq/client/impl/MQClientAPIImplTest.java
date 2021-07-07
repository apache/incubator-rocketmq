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
package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

@RunWith(MockitoJUnitRunner.class)
public class MQClientAPIImplTest {
    private MQClientAPIImpl mqClientAPI = new MQClientAPIImpl(new NettyClientConfig(), null, null, new ClientConfig());
    @Mock
    private RemotingClient remotingClient;
    @Mock
    private DefaultMQProducerImpl defaultMQProducerImpl;

    private String brokerAddr = "127.0.0.1";
    private String brokerName = "DefaultBroker";
    private static String group = "FooBarGroup";
    private static String topic = "FooBar";
    private Message msg = new Message("FooBar", new byte[] {});

    @Before
    public void init() throws Exception {
        Field field = MQClientAPIImpl.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(mqClientAPI, remotingClient);
    }

    @Test
    public void testSendMessageOneWay_Success() throws RemotingException, InterruptedException, MQBrokerException {
        doNothing().when(remotingClient).invokeOneway(anyString(), any(RemotingCommand.class), anyLong());
        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
            3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
        assertThat(sendResult).isNull();
    }

    @Test
    public void testSendMessageOneWay_WithException() throws RemotingException, InterruptedException, MQBrokerException {
        doThrow(new RemotingTimeoutException("Remoting Exception in Test")).when(remotingClient).invokeOneway(anyString(), any(RemotingCommand.class), anyLong());
        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
                3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(RemotingException.class);
        } catch (RemotingException e) {
            assertThat(e).hasMessage("Remoting Exception in Test");
        }

        doThrow(new InterruptedException("Interrupted Exception in Test")).when(remotingClient).invokeOneway(anyString(), any(RemotingCommand.class), anyLong());
        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
                3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(InterruptedException.class);
        } catch (InterruptedException e) {
            assertThat(e).hasMessage("Interrupted Exception in Test");
        }
    }

    @Test
    public void testSendMessageSync_Success() throws InterruptedException, RemotingException, MQBrokerException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createSuccessResponse(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();

        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, requestHeader,
            3 * 1000, CommunicationMode.SYNC, new SendMessageContext(), defaultMQProducerImpl);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
        assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
    }

    @Test
    public void testSendMessageSync_WithException() throws InterruptedException, RemotingException, MQBrokerException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                response.setRemark("Broker is broken.");
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();

        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, requestHeader,
                3 * 1000, CommunicationMode.SYNC, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(MQBrokerException.class);
        } catch (MQBrokerException e) {
            assertThat(e).hasMessageContaining("Broker is broken.");
        }
    }

    @Test
    public void testSendMessageAsync_Success() throws RemotingException, InterruptedException, MQBrokerException {
        doNothing().when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
            3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
        assertThat(sendResult).isNull();

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                InvokeCallback callback = mock.getArgument(3);
                RemotingCommand request = mock.getArgument(1);
                ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
                responseFuture.setResponseCommand(createSuccessResponse(request));
                callback.operationComplete(responseFuture);
                return null;
            }
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        SendMessageContext sendMessageContext = new SendMessageContext();
        sendMessageContext.setProducer(new DefaultMQProducerImpl(new DefaultMQProducer()));
        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
            new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                    assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                    assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
                    assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
                }

                @Override
                public void onException(Throwable e) {
                }
            },
            null, null, 0, sendMessageContext, defaultMQProducerImpl);
    }

    @Test
    public void testSendMessageAsync_WithException() throws RemotingException, InterruptedException, MQBrokerException {
        doThrow(new RemotingTimeoutException("Remoting Exception in Test")).when(remotingClient)
            .invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
                3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(RemotingException.class);
        } catch (RemotingException e) {
            assertThat(e).hasMessage("Remoting Exception in Test");
        }

        doThrow(new InterruptedException("Interrupted Exception in Test")).when(remotingClient)
            .invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
                3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(InterruptedException.class);
        } catch (InterruptedException e) {
            assertThat(e).hasMessage("Interrupted Exception in Test");
        }
    }

    @Test
    public void testCreatePlainAccessConfig_Success() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createSuccessResponse4UpdateAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        PlainAccessConfig config = createUpdateAclConfig();

        try {
            mqClientAPI.createPlainAccessConfig(brokerAddr, config, 3 * 1000);
        } catch (MQClientException ex) {

        }
    }

    @Test
    public void testCreatePlainAccessConfig_Exception() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createErrorResponse4UpdateAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        PlainAccessConfig config = createUpdateAclConfig();
        try {
            mqClientAPI.createPlainAccessConfig(brokerAddr, config, 3 * 1000);
        } catch (MQClientException ex) {
            assertThat(ex.getResponseCode()).isEqualTo(209);
            assertThat(ex.getErrorMessage()).isEqualTo("corresponding to accessConfig has been updated failed");
        }
    }

    @Test
    public void testDeleteAccessConfig_Success() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createSuccessResponse4DeleteAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        String accessKey = "1234567";
        try {
            mqClientAPI.deleteAccessConfig(brokerAddr, accessKey, 3 * 1000);
        } catch (MQClientException ex) {

        }
    }

    @Test
    public void testDeleteAccessConfig_Exception() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createErrorResponse4DeleteAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        try {
            mqClientAPI.deleteAccessConfig(brokerAddr, "11111", 3 * 1000);
        } catch (MQClientException ex) {
            assertThat(ex.getResponseCode()).isEqualTo(210);
            assertThat(ex.getErrorMessage()).isEqualTo("corresponding to accessConfig has been deleted failed");
        }
    }

    @Test
    public void testResumeCheckHalfMessage_WithException() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                response.setRemark("Put message back to RMQ_SYS_TRANS_HALF_TOPIC failed.");
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        boolean result = mqClientAPI.resumeCheckHalfMessage(brokerAddr, "test", 3000);
        assertThat(result).isEqualTo(false);
    }

    @Test
    public void testResumeCheckHalfMessage_Success() throws InterruptedException, RemotingException, MQBrokerException, MQClientException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createResumeSuccessResponse(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        boolean result = mqClientAPI.resumeCheckHalfMessage(brokerAddr, "test", 3000);

        assertThat(result).isEqualTo(true);
    }

    @Test
    public void testSendMessageTypeofReply() throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                InvokeCallback callback = mock.getArgument(3);
                RemotingCommand request = mock.getArgument(1);
                ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
                responseFuture.setResponseCommand(createSuccessResponse(request));
                callback.operationComplete(responseFuture);
                return null;
            }
        }).when(remotingClient).invokeAsync(Matchers.anyString(), Matchers.any(RemotingCommand.class), Matchers.anyLong(), Matchers.any(InvokeCallback.class));
        SendMessageContext sendMessageContext = new SendMessageContext();
        sendMessageContext.setProducer(new DefaultMQProducerImpl(new DefaultMQProducer()));
        msg.getProperties().put("MSG_TYPE", "reply");
        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
            new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                    assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                    assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
                    assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
                }

                @Override
                public void onException(Throwable e) {
                }
            }, null, null, 0, sendMessageContext, defaultMQProducerImpl);
    }

    private RemotingCommand createResumeSuccessResponse(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand createSuccessResponse(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        responseHeader.setMsgId("123");
        responseHeader.setQueueId(1);
        responseHeader.setQueueOffset(123L);

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, "RegionHZ");
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, "true");
        response.addExtField("queueId", String.valueOf(responseHeader.getQueueId()));
        response.addExtField("msgId", responseHeader.getMsgId());
        response.addExtField("queueOffset", String.valueOf(responseHeader.getQueueOffset()));
        return response;
    }

    private RemotingCommand createSuccessResponse4UpdateAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark(null);

        return response;
    }

    private RemotingCommand createSuccessResponse4DeleteAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark(null);

        return response;
    }

    private RemotingCommand createErrorResponse4UpdateAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark("corresponding to accessConfig has been updated failed");

        return response;
    }

    private RemotingCommand createErrorResponse4DeleteAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.DELETE_ACL_CONFIG_FAILED);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark("corresponding to accessConfig has been deleted failed");

        return response;
    }

    private PlainAccessConfig createUpdateAclConfig() {

        PlainAccessConfig config = new PlainAccessConfig();
        config.setAccessKey("Rocketmq111");
        config.setSecretKey("123456789");
        config.setAdmin(true);
        config.setWhiteRemoteAddress("127.0.0.1");
        config.setDefaultTopicPerm("DENY");
        config.setDefaultGroupPerm("SUB");
        return config;
    }

    private SendMessageRequestHeader createSendMessageRequestHeader() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setTopic(topic);
        requestHeader.setProducerGroup(group);
        requestHeader.setQueueId(1);
        requestHeader.setMaxReconsumeTimes(10);
        return requestHeader;
    }

    @Test
    public void testAddWritePermOfBroker() throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                RemotingCommand request = invocationOnMock.getArgument(1);
                if (request.getCode() != RequestCode.ADD_WRITE_PERM_OF_BROKER) {
                    return null;
                }

                RemotingCommand response = RemotingCommand.createResponseCommand(AddWritePermOfBrokerResponseHeader.class);
                AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.readCustomHeader();
                response.setCode(ResponseCode.SUCCESS);
                responseHeader.setAddTopicCount(7);
                response.addExtField("addTopicCount", String.valueOf(responseHeader.getAddTopicCount()));
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        int topicCnt = mqClientAPI.addWritePermOfBroker("127.0.0.1", "default-broker", 1000);
        assertThat(topicCnt).isEqualTo(7);
    }
}