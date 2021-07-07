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

import com.google.common.collect.Sets;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetByOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AdminBrokerProcessorTest {

    private AdminBrokerProcessor adminBrokerProcessor;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Spy
    private BrokerController
            brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
            new MessageStoreConfig());

    @Mock
    private MessageStore messageStore;
    private Broker2Client broker2Client;

    private Set<String> systemTopicSet;

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        adminBrokerProcessor = new AdminBrokerProcessor(brokerController);
        broker2Client = mock(Broker2Client.class);

        systemTopicSet = Sets.newHashSet(
                TopicValidator.RMQ_SYS_SELF_TEST_TOPIC,
                TopicValidator.RMQ_SYS_BENCHMARK_TOPIC,
                TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT,
                TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC,
                this.brokerController.getBrokerConfig().getBrokerClusterName(),
                this.brokerController.getBrokerConfig().getBrokerClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX);
        if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
            systemTopicSet.add(this.brokerController.getBrokerConfig().getMsgTraceTopicName());
        }
    }

    @Test
    public void testProcessRequest_success() throws RemotingCommandException, UnknownHostException {
        RemotingCommand request = createResumeCheckHalfMessageCommand();
        when(messageStore.selectOneMessageByOffset(any(Long.class))).thenReturn(createSelectMappedBufferResult());
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult
                (PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_fail() throws RemotingCommandException, UnknownHostException {
        RemotingCommand request = createResumeCheckHalfMessageCommand();
        when(messageStore.selectOneMessageByOffset(any(Long.class))).thenReturn(createSelectMappedBufferResult());
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult
                (PutMessageStatus.UNKNOWN_ERROR, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testResetOffsetByOffset() throws Exception {
        RemotingCommand notOnlineResponse = RemotingCommand.createResponseCommand(ResponseCode.CONSUMER_NOT_ONLINE, "notOnlineRemark");

        ResetOffsetByOffsetRequestHeader header = new ResetOffsetByOffsetRequestHeader();
        header.setOffset(1);
        header.setQueueId(0);
        header.setGroup("group");
        header.setTopic("topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET_BY_OFFSET, header);
        request.makeCustomHeaderToNet();

        when(brokerController.getBroker2Client()).thenReturn(broker2Client);
        when(broker2Client.resetOffsetByOffset(anyString(), anyString(), anyInt(), anyLong(), anyBoolean())).thenReturn(notOnlineResponse);

        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getRemark()).isEqualToIgnoringCase("[reset-offset] consumer not online, so only update the broker's offset");
    }

    @Test
    public void testUpdateAndCreateTopic() throws Exception {
        //test system topic
        for (String topic : systemTopicSet) {
            RemotingCommand request = buildCreateTopicRequest(topic);
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        //test validate error topic
        String topic = "";
        RemotingCommand request = buildCreateTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        topic = "TEST_CREATE_TOPIC";
        request = buildCreateTopicRequest(topic);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

    }

    @Test
    public void testDeleteTopic() throws Exception {
        //test system topic
        for (String topic : systemTopicSet) {
            RemotingCommand request = buildDeleteTopicRequest(topic);
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        String topic = "TEST_DELETE_TOPIC";
        RemotingCommand request = buildDeleteTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private RemotingCommand buildCreateTopicRequest(String topic) {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setTopicFilterType(TopicFilterType.SINGLE_TAG.name());
        requestHeader.setReadQueueNums(8);
        requestHeader.setWriteQueueNums(8);
        requestHeader.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand buildDeleteTopicRequest(String topic) {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private SelectMappedBufferResult createSelectMappedBufferResult() {
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, ByteBuffer.allocate(1024), 0, new MappedFile());
        return result;
    }

    private ResumeCheckHalfMessageRequestHeader createResumeCheckHalfMessageRequestHeader() {
        ResumeCheckHalfMessageRequestHeader header = new ResumeCheckHalfMessageRequestHeader();
        header.setMsgId("C0A803CA00002A9F0000000000031367");
        return header;
    }

    private RemotingCommand createResumeCheckHalfMessageCommand() {
        ResumeCheckHalfMessageRequestHeader header = createResumeCheckHalfMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, header);
        request.makeCustomHeaderToNet();
        return request;
    }
}
