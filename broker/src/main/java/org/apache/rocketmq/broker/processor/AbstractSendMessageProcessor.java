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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.domain.LogicalQueuesInfoInBroker;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.TopicQueueId;
import org.apache.rocketmq.common.constant.DBMsgConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.route.LogicalQueueRouteData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ChannelUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.srvutil.ConcurrentHashMapUtil;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

public abstract class AbstractSendMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    protected final SocketAddress storeHost;
    private List<SendMessageHook> sendMessageHookList;

    private final ConcurrentMap<TopicQueueId, LongAdder> inFlyWritingCounterMap = Maps.newConcurrentMap();

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost =
            new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                .getNettyServerConfig().getListenPort());
    }

    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx,
        SendMessageRequestHeader requestHeader) {
        if (!this.hasSendMessageHook()) {
            return null;
        }
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic());
        SendMessageContext mqtraceContext;
        mqtraceContext = new SendMessageContext();
        mqtraceContext.setProducerGroup(requestHeader.getProducerGroup());
        mqtraceContext.setNamespace(namespace);
        mqtraceContext.setTopic(requestHeader.getTopic());
        mqtraceContext.setMsgProps(requestHeader.getProperties());
        mqtraceContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        mqtraceContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        mqtraceContext.setBrokerRegionId(this.brokerController.getBrokerConfig().getRegionId());
        mqtraceContext.setBornTimeStamp(requestHeader.getBornTimestamp());

        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String uniqueKey = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        properties.put(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        properties.put(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));
        requestHeader.setProperties(MessageDecoder.messageProperties2String(properties));

        if (uniqueKey == null) {
            uniqueKey = "";
        }
        mqtraceContext.setMsgUniqueKey(uniqueKey);
        return mqtraceContext;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }

    protected MessageExtBrokerInner buildInnerMsg(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final byte[] body, TopicConfig topicConfig) {
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }
        int sysFlag = requestHeader.getSysFlag();

        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
            MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader
            .getReconsumeTimes());
        return msgInner;
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, RemotingCommand request,
        final RemotingCommand response) {
        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long {}", requestHeader.getTopic().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long {}", requestHeader.getProperties().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (request.getBody().length > DBMsgConstants.MAX_BODY_SIZE) {
            log.warn(" topic {}  msg body size {}  from {}", requestHeader.getTopic(),
                request.getBody().length, ChannelUtil.getRemoteIp(ctx.channel()));
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

    protected RemotingCommand msgCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final RemotingCommand response) {
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending message is forbidden");
            return response;
        }

        if (!TopicValidator.validateTopic(requestHeader.getTopic(), response)) {
            return response;
        }
        if (TopicValidator.isNotAllowedSendTopic(requestHeader.getTopic(), response)) {
            return response;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic {} not exist, producer: {}", requestHeader.getTopic(), ctx.channel().remoteAddress());
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                requestHeader.getTopic(),
                requestHeader.getDefaultTopic(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig =
                        this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                            requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                            topicSysFlag);
                }
            }

            if (null == topicConfig) {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s",
                queueIdInt,
                topicConfig.toString(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);

            return response;
        }
        return response;
    }

    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }

    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request,
        final RemotingCommand response) {
        if (!request.isOnewayRPC()) {
            try {
                ctx.writeAndFlush(response);
            } catch (Throwable e) {
                log.error("SendMessageProcessor process request over, but response failed", e);
                log.error(request.toString());
                log.error(response.toString());
            }
        }
    }

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final RemotingCommand request,
        SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    final SendMessageRequestHeader requestHeader = parseRequestHeader(request);

                    String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic());
                    if (null != requestHeader) {
                        context.setNamespace(namespace);
                        context.setProducerGroup(requestHeader.getProducerGroup());
                        context.setTopic(requestHeader.getTopic());
                        context.setBodyLength(request.getBody().length);
                        context.setMsgProps(requestHeader.getProperties());
                        context.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                        context.setBrokerAddr(this.brokerController.getBrokerAddr());
                        context.setQueueId(requestHeader.getQueueId());
                    }

                    hook.sendMessageBefore(context);
                    if (requestHeader != null) {
                        requestHeader.setProperties(context.getMsgProps());
                    }
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request)
        throws RemotingCommandException {

        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_BATCH_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
                requestHeaderV2 =
                    (SendMessageRequestHeaderV2) request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader =
                        (SendMessageRequestHeader) request
                            .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader =
                            (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }


    public ConcurrentMap<TopicQueueId, LongAdder> getInFlyWritingCounterMap() {
        return inFlyWritingCounterMap;
    }

    protected LogicalQueueContext buildLogicalQueueContext(String topic, int queueId,
        RemotingCommand response) {
        TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();
        LogicalQueuesInfoInBroker logicalQueuesInfo = topicConfigManager.selectLogicalQueuesInfo(topic);
        if (logicalQueuesInfo == null) {
            return noopLogicalQueueContext;
        }
        // writable route data will has largest offset
        LogicalQueueRouteData curQueueRouteData = logicalQueuesInfo.queryQueueRouteDataByQueueId(queueId, Long.MAX_VALUE);
        if (curQueueRouteData == null) {
            // topic enabled logical queue, but some message queues are not converted or being converted
            String msg = String.format(Locale.ENGLISH, "queueId %d not included in logical queue", queueId);
            log.debug("buildLogicalQueueContext unexpected error, topic {} {}", topic, msg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(msg);
            return noopLogicalQueueContext;
        }
        LongAdder inFlyWritingCounter = ConcurrentHashMapUtil.computeIfAbsent(inFlyWritingCounterMap, new TopicQueueId(topic, queueId), ignore -> new LongAdder());
        return new LogicalQueueContext(topic, queueId, logicalQueuesInfo, curQueueRouteData, inFlyWritingCounter);
    }

    protected class LogicalQueueContext {
        private final String topic;
        private final int queueId;
        private final LogicalQueuesInfoInBroker logicalQueuesInfo;
        private final LogicalQueueRouteData curQueueRouteData;
        private final LongAdder inFlyWritingCounter;

        public LogicalQueueContext(String topic, int queueId,
            LogicalQueuesInfoInBroker logicalQueuesInfo,
            LogicalQueueRouteData curQueueRouteData, LongAdder inFlyWritingCounter) {
            this.topic = topic;
            this.queueId = queueId;
            this.logicalQueuesInfo = logicalQueuesInfo;
            this.curQueueRouteData = curQueueRouteData;
            this.inFlyWritingCounter = inFlyWritingCounter;
        }

        public CompletableFuture<RemotingCommand> hookBeforePut(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader,
            RemotingCommand request, RemotingCommand response) {
            if (curQueueRouteData.isWritable()) {
                this.inFlyWritingCounter.increment();
                return null;
            }
            int logicalQueueIdx = curQueueRouteData.getLogicalQueueIndex();
            List<LogicalQueueRouteData> queueRouteDataList = logicalQueuesInfo.get(logicalQueueIdx);
            LogicalQueueRouteData writableQueueRouteData = null;
            for (int i = queueRouteDataList.size() - 1; i >= 0; i--) {
                LogicalQueueRouteData queueRouteData = queueRouteDataList.get(i);
                if (queueRouteData.isWritable()) {
                    writableQueueRouteData = queueRouteData;
                    break;
                }
            }
            if (writableQueueRouteData == null) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(String.format(Locale.ENGLISH, "broker[%s] topic[%s] queueId[%d] logicalQueueIdx[%d] not writable", AbstractSendMessageProcessor.this.brokerController.getBrokerConfig().getBrokerIP1(), topic, queueId, logicalQueueIdx));
                return CompletableFuture.completedFuture(response);
            }
            if ((Optional.ofNullable(requestHeader.getSysFlag()).orElse(0) & MessageSysFlag.LOGICAL_QUEUE_FLAG) > 0) {
                // new client, use redirect
                response.setCode(ResponseCode.NO_PERMISSION);
                response.addExtField(MessageConst.PROPERTY_REDIRECT, "1");
                response.setBody(RemotingSerializable.encode(ImmutableList.of(curQueueRouteData, writableQueueRouteData)));
                return CompletableFuture.completedFuture(response);
            } else {
                // old client, use forward
                this.logicalQueueHookForward(ctx, writableQueueRouteData, requestHeader, request, response);
            }
            if (response.getCode() != -1) {
                return CompletableFuture.completedFuture(response);
            } else if (response.getCode() == ResponseCode.ASYNC_AND_RETURN_NULL) {
                return CompletableFuture.completedFuture(null);
            }
            return null;
        }

        private void logicalQueueHookForward(ChannelHandlerContext ctx,
            LogicalQueueRouteData writableQueueRouteData,
            SendMessageRequestHeader requestHeader, RemotingCommand request,
            RemotingCommand response) {
            response.setCode(ResponseCode.SUCCESS);
            requestHeader.setQueueId(writableQueueRouteData.getQueueId());
            request.writeCustomHeader(requestHeader);
            String brokerName = writableQueueRouteData.getBrokerName();
            BrokerController brokerController = AbstractSendMessageProcessor.this.brokerController;
            String brokerAddr = brokerController.getBrokerAddrByName(brokerName);
            if (brokerAddr == null) {
                log.warn("getBrokerAddrByName brokerName={} got null, fallback to queueRouteData.getBrokerAddr()", brokerName);
                brokerAddr = writableQueueRouteData.getBrokerAddr();
            }
            if (brokerAddr == null) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                String msg = String.format(Locale.ENGLISH, "unknown brokerName %s", brokerName);
                response.setRemark(msg);
                log.warn("logicalQueueHookForward can not look up brokerName={}: {}", brokerName, requestHeader);
                return;
            }
            try {
                String finalBrokerAddr = brokerAddr;
                brokerController.getBrokerOuterAPI().forwardRequest(brokerAddr, request, brokerController.getBrokerConfig().getForwardTimeout(), responseFuture -> {
                    RemotingCommand forwardResponse = responseFuture.getResponseCommand();
                    if (forwardResponse == null) {
                        forwardResponse = response;
                        forwardResponse.setCode(ResponseCode.SYSTEM_ERROR);
                        if (!responseFuture.isSendRequestOK()) {
                            forwardResponse.setRemark(String.format(Locale.ENGLISH, "send request failed to %s: %s", finalBrokerAddr, responseFuture.getCause()));
                        } else if (responseFuture.isTimeout()) {
                            forwardResponse.setRemark(String.format(Locale.ENGLISH, "wait response from  %s timeout: %dms", finalBrokerAddr, responseFuture.getTimeoutMillis()));
                        } else {
                            forwardResponse.setRemark(String.format(Locale.ENGLISH, "unknown reason. addr: %s, timeoutMillis: %d: %s", finalBrokerAddr, responseFuture.getTimeoutMillis(), responseFuture.getCause()));
                        }
                    } else {
                        CommandCustomHeader customHeader = forwardResponse.readCustomHeader();
                        if (customHeader instanceof SendMessageResponseHeader) {
                            SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) customHeader;
                            Integer forwardQueueId = responseHeader.getQueueId();
                            forwardResponse.addExtField(MessageConst.PROPERTY_FORWARD_QUEUE_ID, forwardQueueId != null ? Integer.toString(forwardQueueId) : "null");
                            responseHeader.setQueueId(requestHeader.getQueueId());
                            // queueOffset should not be changed since forwarded broker will add delta to it.
                        }
                    }
                    AbstractSendMessageProcessor.this.doResponse(ctx, request, forwardResponse);
                });
                response.setCode(ResponseCode.ASYNC_AND_RETURN_NULL);
            } catch (Exception e) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("forward error");
                log.warn(String.format(Locale.ENGLISH, "logicalQueueHookForward to %s error", brokerAddr), e);
            }
        }

        public void hookAfterPut(CompletableFuture<PutMessageResult> putMessageResult) {
            Optional.ofNullable(putMessageResult).orElse(CompletableFuture.completedFuture(null)).whenComplete((result, throwable) -> {
                this.inFlyWritingCounter.decrement();
            });
        }
    }

    private final LogicalQueueContext noopLogicalQueueContext = new LogicalQueueContext(null, 0, null, null, null) {
        @Override public CompletableFuture<RemotingCommand> hookBeforePut(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader,
            RemotingCommand request, RemotingCommand response) {
            return null;
        }

        @Override public void hookAfterPut(CompletableFuture<PutMessageResult> putMessageResult) {
        }
    };
}
