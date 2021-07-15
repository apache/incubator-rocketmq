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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.domain.LogicalQueuesInfoInBroker;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.TopicQueueId;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CreateMessageQueueForLogicalQueueRequestBody;
import org.apache.rocketmq.common.protocol.body.MigrateLogicalQueueBody;
import org.apache.rocketmq.common.protocol.body.ReuseTopicLogicalQueueRequestBody;
import org.apache.rocketmq.common.protocol.body.SealTopicLogicalQueueRequestBody;
import org.apache.rocketmq.common.protocol.body.UpdateTopicLogicalQueueMappingRequestBody;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicLogicalQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryTopicLogicalQueueMappingRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.protocol.route.MessageQueueRouteState;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AdminBrokerProcessorTest {

    private AdminBrokerProcessor adminBrokerProcessor;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private Channel channel;

    @Spy
    private BrokerController
        brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
        new MessageStoreConfig());

    @Mock
    private MessageStore messageStore;

    @Mock
    private SendMessageProcessor sendMessageProcessor;

    @Mock
    private ConcurrentMap<TopicQueueId, LongAdder> inFlyWritingCouterMap;

    private Set<String> systemTopicSet;
    private String topic;

    @Before
    public void init() throws Exception {
        brokerController.setMessageStore(messageStore);

        doReturn(sendMessageProcessor).when(brokerController).getSendMessageProcessor();
        when(sendMessageProcessor.getInFlyWritingCounterMap()).thenReturn(inFlyWritingCouterMap);

        adminBrokerProcessor = new AdminBrokerProcessor(brokerController);

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
        when(handlerContext.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));

        topic = "FooBar" + System.nanoTime();
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        topicConfigManager.updateTopicConfig(new TopicConfig(topic));
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

    @Test
    public void testGetTopicConfig() throws Exception {
        String topic = "foobar";
        brokerController.getTopicConfigManager().updateTopicConfig(new TopicConfig(topic));

        {
            GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
            requestHeader.setTopic(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(response.getBody()).isNotEmpty();
        }
        {
            GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
            requestHeader.setTopic("aaaaaaa");
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).contains("No topic in this broker.");
        }
    }

    @Test
    public void testUpdateTopicLogicalQueueMapping() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;

        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_TOPIC_LOGICAL_QUEUE_MAPPING, null);
        UpdateTopicLogicalQueueMappingRequestBody requestBody = new UpdateTopicLogicalQueueMappingRequestBody();
        requestBody.setTopic(topic);
        requestBody.setQueueId(queueId);
        requestBody.setLogicalQueueIdx(logicalQueueIndex);
        request.setBody(requestBody.encode());
        RemotingCommand response;
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(topicConfigManager.getOrCreateLogicalQueuesInfo(topic).get(logicalQueueIndex).get(0)).isEqualTo(new LogicalQueueRouteData(logicalQueueIndex, 0L, new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId), MessageQueueRouteState.Normal, 0L, -1, -1, -1, brokerController.getBrokerAddr()));

        // delete
        requestBody.setLogicalQueueIdx(-1);
        request.setBody(requestBody.encode());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(topicConfigManager.getOrCreateLogicalQueuesInfo(topic).get(logicalQueueIndex)).isEmpty();
        verify(inFlyWritingCouterMap).remove(new TopicQueueId(topic, queueId));
    }

    @Test
    public void testDeleteTopicLogicalQueueMapping() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(logicalQueueIndex, 0L, new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId), MessageQueueRouteState.Normal, 0L, -1, -1, -1, brokerController.getBrokerAddr())));

        DeleteTopicLogicalQueueRequestHeader requestHeader = new DeleteTopicLogicalQueueRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_LOGICAL_QUEUE_MAPPING, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("still 1 message queues");

        logicalQueuesInfo.remove(logicalQueueIndex);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(topicConfigManager.selectLogicalQueuesInfo(topic)).isNull();
    }

    @Test
    public void testQueryTopicLogicalQueueMapping() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(logicalQueueIndex, 0L, new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId), MessageQueueRouteState.Normal, 0L, -1, -1, -1, brokerController.getBrokerAddr())));

        QueryTopicLogicalQueueMappingRequestHeader requestHeader = new QueryTopicLogicalQueueMappingRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_LOGICAL_QUEUE_MAPPING, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        Map<Integer, List<LogicalQueueRouteData>> m = JSON.parseObject(response.getBody(), new TypeReference<Map<Integer, List<LogicalQueueRouteData>>>() {
        }.getType());
        assertThat(m.get(logicalQueueIndex)).isEqualTo(logicalQueuesInfo.get(logicalQueueIndex));
    }

    @Test
    public void testSealTopicLogicalQueue() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        MessageQueue mq = new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId);
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(logicalQueueIndex, 0L, mq, MessageQueueRouteState.Normal, 0L, -1, -1, -1, brokerController.getBrokerAddr())));

        when(messageStore.getMaxOffsetInQueue(eq(topic), eq(queueId), anyBoolean())).thenReturn(100L);
        when(messageStore.getMinOffsetInQueue(eq(topic), eq(queueId))).thenReturn(0L);
        when(messageStore.getMinPhyOffset()).thenReturn(1000L);
        when(messageStore.getCommitLogOffsetInQueue(eq(topic), eq(queueId), eq(0L))).thenReturn(2000L);
        when(messageStore.getCommitLogOffsetInQueue(eq(topic), eq(queueId), eq(99L))).thenReturn(3000L);
        MessageExt firstMsg = mock(MessageExt.class);
        when(firstMsg.getStoreTimestamp()).thenReturn(200L);
        when(messageStore.lookMessageByOffset(eq(2000L))).thenReturn(firstMsg);
        MessageExt lastMsg = mock(MessageExt.class);
        when(lastMsg.getStoreTimestamp()).thenReturn(300L);
        when(messageStore.lookMessageByOffset(eq(3000L))).thenReturn(lastMsg);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEAL_TOPIC_LOGICAL_QUEUE, null);
        SealTopicLogicalQueueRequestBody requestBody = new SealTopicLogicalQueueRequestBody();
        requestBody.setTopic(topic);
        requestBody.setQueueId(queueId);
        requestBody.setLogicalQueueIndex(logicalQueueIndex);
        request.setBody(requestBody.encode());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        LogicalQueueRouteData wantLogicalQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 0L, mq, MessageQueueRouteState.ReadOnly, 0, 100, 200, 300, brokerController.getBrokerAddr());
        assertThat(logicalQueuesInfo.get(logicalQueueIndex).get(0)).isEqualTo(wantLogicalQueueRouteData);
        assertThat((LogicalQueueRouteData) JSON.parseObject(response.getBody(), LogicalQueueRouteData.class)).isEqualTo(wantLogicalQueueRouteData);

        // expired
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(logicalQueueIndex, 0L, mq, MessageQueueRouteState.Normal, 0L, -1, -1, -1, brokerController.getBrokerAddr())));
        when(messageStore.getMinPhyOffset()).thenReturn(10000L);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        wantLogicalQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 0L, mq, MessageQueueRouteState.Expired, 0, 100, 0, 0, brokerController.getBrokerAddr());
        assertThat(logicalQueuesInfo.get(logicalQueueIndex).get(0)).isEqualTo(wantLogicalQueueRouteData);
        assertThat((LogicalQueueRouteData) JSON.parseObject(response.getBody(), LogicalQueueRouteData.class)).isEqualTo(wantLogicalQueueRouteData);

        // expired and empty
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(logicalQueueIndex, 0L, mq, MessageQueueRouteState.Normal, 0L, -1, -1, -1, brokerController.getBrokerAddr())));
        when(messageStore.getMinOffsetInQueue(eq(topic), eq(queueId))).thenReturn(100L);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        wantLogicalQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 0L, mq, MessageQueueRouteState.Expired, 0, 100, 0, 0, brokerController.getBrokerAddr());
        assertThat(logicalQueuesInfo.get(logicalQueueIndex).get(0)).isEqualTo(wantLogicalQueueRouteData);
        assertThat((LogicalQueueRouteData) JSON.parseObject(response.getBody(), LogicalQueueRouteData.class)).isEqualTo(wantLogicalQueueRouteData);
    }

    @Test
    public void testReuseTopicLogicalQueue() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        MessageQueue mq = new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId);
        LogicalQueueRouteData logicalQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 500L, mq, MessageQueueRouteState.Expired, 100L, 200L, 300L, 400L, brokerController.getBrokerAddr());
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(logicalQueueRouteData));
        LogicalQueueRouteData wantData0 = new LogicalQueueRouteData(logicalQueueRouteData);

        when(messageStore.getMaxOffsetInQueue(eq(topic), eq(queueId), anyBoolean())).thenReturn(600L);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REUSE_TOPIC_LOGICAL_QUEUE, null);
        ReuseTopicLogicalQueueRequestBody requestBody = new ReuseTopicLogicalQueueRequestBody();
        requestBody.setTopic(topic);
        requestBody.setQueueId(queueId);
        requestBody.setLogicalQueueIndex(logicalQueueIndex);
        requestBody.setMessageQueueRouteState(MessageQueueRouteState.WriteOnly);
        request.setBody(requestBody.encode());

        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        LogicalQueueRouteData wantData1 = new LogicalQueueRouteData(logicalQueueIndex, -1L, mq, MessageQueueRouteState.WriteOnly, 600L, -1, -1, -1, brokerController.getBrokerAddr());
        assertThat((LogicalQueueRouteData) JSON.parseObject(response.getBody(), LogicalQueueRouteData.class)).isEqualTo(wantData1);
        assertThat(logicalQueuesInfo.get(logicalQueueIndex)).isEqualTo(Arrays.asList(wantData0, wantData1));
        verify(inFlyWritingCouterMap).remove(new TopicQueueId(topic, queueId));
    }

    @Test
    public void testCreateMessageQueueForLogicalQueue() throws Exception {
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        topicConfig.setWriteQueueNums(0);
        topicConfig.setReadQueueNums(0);
        int queueId = 0;
        assertThat(topicConfigManager.selectLogicalQueuesInfo(topic)).isNull();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CREATE_MESSAGE_QUEUE_FOR_LOGICAL_QUEUE, null);
        CreateMessageQueueForLogicalQueueRequestBody requestBody = new CreateMessageQueueForLogicalQueueRequestBody();
        requestBody.setTopic(topic);
        requestBody.setLogicalQueueIndex(logicalQueueIndex);
        requestBody.setMessageQueueStatus(MessageQueueRouteState.WriteOnly);
        request.setBody(requestBody.encode());

        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).withFailMessage("remark: %s", response.getRemark()).isEqualTo(ResponseCode.SUCCESS);
        LogicalQueueRouteData wantLogicalQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, -1L, new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId), MessageQueueRouteState.WriteOnly, 0L, -1, -1, -1, brokerController.getBrokerAddr());
        assertThat((LogicalQueueRouteData) JSON.parseObject(response.getBody(), LogicalQueueRouteData.class)).isEqualTo(wantLogicalQueueRouteData);
        assertThat(topicConfigManager.selectLogicalQueuesInfo(topic).get(logicalQueueIndex).get(0)).isEqualTo(wantLogicalQueueRouteData);
    }

    @Test
    public void testMigrateTopicLogicalQueuePrepare() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        MessageQueue mq = new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId);
        LogicalQueueRouteData fromQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 500L, mq, MessageQueueRouteState.Normal, 10L, -1L, -1L, -1L, brokerController.getBrokerAddr());
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(fromQueueRouteData)));

        when(messageStore.getMaxOffsetInQueue(eq(topic), eq(queueId), anyBoolean())).thenReturn(100L);
        when(messageStore.getMinOffsetInQueue(eq(topic), eq(queueId))).thenReturn(10L);
        when(messageStore.getMinPhyOffset()).thenReturn(1000L);
        when(messageStore.getCommitLogOffsetInQueue(eq(topic), eq(queueId), eq(10L))).thenReturn(2000L);
        when(messageStore.getCommitLogOffsetInQueue(eq(topic), eq(queueId), eq(99L))).thenReturn(3000L);
        MessageExt firstMsg = mock(MessageExt.class);
        when(firstMsg.getStoreTimestamp()).thenReturn(200L);
        when(messageStore.lookMessageByOffset(eq(2000L))).thenReturn(firstMsg);
        MessageExt lastMsg = mock(MessageExt.class);
        when(lastMsg.getStoreTimestamp()).thenReturn(300L);
        when(messageStore.lookMessageByOffset(eq(3000L))).thenReturn(lastMsg);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MIGRATE_TOPIC_LOGICAL_QUEUE_PREPARE, null);
        MigrateLogicalQueueBody requestBody = new MigrateLogicalQueueBody();
        requestBody.setFromQueueRouteData(fromQueueRouteData);
        LogicalQueueRouteData toQueueRouteData = new LogicalQueueRouteData();
        toQueueRouteData.setMessageQueue(new MessageQueue(topic, "toBroker", 1));
        requestBody.setToQueueRouteData(toQueueRouteData);
        request.setBody(requestBody.encode());

        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).withFailMessage("remark: %s", response.getRemark()).isEqualTo(ResponseCode.SUCCESS);
        fromQueueRouteData.setState(MessageQueueRouteState.ReadOnly);
        fromQueueRouteData.setOffsetMax(100L);
        fromQueueRouteData.setFirstMsgTimeMillis(200L);
        fromQueueRouteData.setLastMsgTimeMillis(300L);
        toQueueRouteData.setLogicalQueueDelta(590L);
        MigrateLogicalQueueBody responseBody = RemotingSerializable.decode(response.getBody(), MigrateLogicalQueueBody.class);
        assertThat(responseBody.getFromQueueRouteData()).isEqualTo(fromQueueRouteData);
        assertThat(responseBody.getToQueueRouteData()).isEqualTo(toQueueRouteData);
        assertThat(logicalQueuesInfo.get(logicalQueueIndex)).isEqualTo(Lists.newArrayList(fromQueueRouteData, toQueueRouteData));
    }

    @Test
    public void testMigrateTopicLogicalQueueCommit() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        MessageQueue mq = new MessageQueue(topic, brokerController.getBrokerConfig().getBrokerName(), queueId);
        LogicalQueueRouteData fromQueueRouteData = new LogicalQueueRouteData();
        fromQueueRouteData.setMessageQueue(new MessageQueue(topic, "fromBroker", 0));
        LogicalQueueRouteData toQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 500L, mq, MessageQueueRouteState.Normal, 500L, -1L, -1L, -1L, brokerController.getBrokerAddr());
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(toQueueRouteData)));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MIGRATE_TOPIC_LOGICAL_QUEUE_COMMIT, null);
        MigrateLogicalQueueBody requestBody = new MigrateLogicalQueueBody();
        requestBody.setFromQueueRouteData(fromQueueRouteData);
        requestBody.setToQueueRouteData(toQueueRouteData);
        request.setBody(requestBody.encode());

        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).withFailMessage("remark: %s", response.getRemark()).isEqualTo(ResponseCode.SUCCESS);
        MigrateLogicalQueueBody responseBody = RemotingSerializable.decode(response.getBody(), MigrateLogicalQueueBody.class);
        assertThat(responseBody.getFromQueueRouteData()).isEqualTo(fromQueueRouteData);
        assertThat(responseBody.getToQueueRouteData()).isEqualTo(toQueueRouteData);
        assertThat(logicalQueuesInfo.get(logicalQueueIndex)).isEqualTo(Lists.newArrayList(toQueueRouteData));
    }

    @Test
    public void testMigrateTopicLogicalQueueNotify() throws Exception {
        int queueId = 0;
        int logicalQueueIndex = 0;
        TopicConfigManager topicConfigManager = brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.getOrCreateLogicalQueuesInfo(topic);
        LogicalQueueRouteData fromQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 100L, new MessageQueue(topic, "fromBroker", queueId), MessageQueueRouteState.ReadOnly, 10L, 410L, 200L, 300L, brokerController.getBrokerAddr());
        LogicalQueueRouteData toQueueRouteData = new LogicalQueueRouteData(logicalQueueIndex, 500L, new MessageQueue(topic, "toBroker", queueId), MessageQueueRouteState.Normal, 500L, -1L, -1L, -1L, brokerController.getBrokerAddr());
        logicalQueuesInfo.put(logicalQueueIndex, Lists.newArrayList(new LogicalQueueRouteData(fromQueueRouteData)));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MIGRATE_TOPIC_LOGICAL_QUEUE_NOTIFY, null);
        MigrateLogicalQueueBody requestBody = new MigrateLogicalQueueBody();
        requestBody.setFromQueueRouteData(fromQueueRouteData);
        requestBody.setToQueueRouteData(toQueueRouteData);
        request.setBody(requestBody.encode());

        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).withFailMessage("remark: %s", response.getRemark()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(logicalQueuesInfo.get(logicalQueueIndex)).isEqualTo(Lists.newArrayList(fromQueueRouteData, toQueueRouteData));
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

    private MessageExt createDefaultMessageExt() {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("12345678");
        messageExt.setQueueId(0);
        messageExt.setCommitLogOffset(123456789L);
        messageExt.setQueueOffset(1234);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_TOPIC, "testTopic");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, "15");
        return messageExt;
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
