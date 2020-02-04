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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ReplyMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public ReplyMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        SendMessageContext mqtraceContext = null;
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
            return null;
        }

        mqtraceContext = buildMsgContext(ctx, requestHeader);
        this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

        RemotingCommand response = this.processReplyMessageRequest(ctx, request, mqtraceContext, requestHeader);

        this.executeSendMessageHookAfter(response, mqtraceContext);
        return response;
    }

    @Override
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_REPLY_MESSAGE_V2:
                requestHeaderV2 =
                    (SendMessageRequestHeaderV2) request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_REPLY_MESSAGE:
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

    private RemotingCommand processReplyMessageRequest(final ChannelHandlerContext ctx,
        final RemotingCommand request,
        final SendMessageContext sendMessageContext,
        final SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendReplyMessage request command, {}", request);
        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

        PushReplyResult pushReplyResult = this.pushReplyMessage(ctx, requestHeader, msgInner);
        this.handlePushReplyResult(pushReplyResult, response, responseHeader, queueIdInt);

        if (this.brokerController.getBrokerConfig().isStoreReplyMessageEnable()) {
            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
            this.handlePutMessageResult(putMessageResult, request, msgInner, responseHeader, sendMessageContext, queueIdInt);
        }

        return response;
    }

    private PushReplyResult pushReplyMessage(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader,
        final Message msg) {
        ReplyMessageRequestHeader replyMessageRequestHeader = new ReplyMessageRequestHeader();
        replyMessageRequestHeader.setBornHost(ctx.channel().remoteAddress().toString());
        replyMessageRequestHeader.setStoreHost(this.getStoreHost().toString());
        replyMessageRequestHeader.setStoreTimestamp(System.currentTimeMillis());
        replyMessageRequestHeader.setProducerGroup(requestHeader.getProducerGroup());
        replyMessageRequestHeader.setTopic(requestHeader.getTopic());
        replyMessageRequestHeader.setDefaultTopic(requestHeader.getDefaultTopic());
        replyMessageRequestHeader.setDefaultTopicQueueNums(requestHeader.getDefaultTopicQueueNums());
        replyMessageRequestHeader.setQueueId(requestHeader.getQueueId());
        replyMessageRequestHeader.setSysFlag(requestHeader.getSysFlag());
        replyMessageRequestHeader.setBornTimestamp(requestHeader.getBornTimestamp());
        replyMessageRequestHeader.setFlag(requestHeader.getFlag());
        replyMessageRequestHeader.setProperties(requestHeader.getProperties());
        replyMessageRequestHeader.setReconsumeTimes(requestHeader.getReconsumeTimes());
        replyMessageRequestHeader.setUnitMode(requestHeader.isUnitMode());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, replyMessageRequestHeader);
        request.setBody(msg.getBody());

        String senderId = msg.getProperties().get(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        PushReplyResult pushReplyResult = new PushReplyResult(false);

        if (senderId != null) {
            Channel channel = this.brokerController.getProducerManager().findChannel(senderId);
            if (channel != null) {
                msg.getProperties().put(MessageConst.PROPERTY_PUSH_REPLY_TIME, String.valueOf(System.currentTimeMillis()));
                replyMessageRequestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

                try {
                    RemotingCommand pushResponse = this.brokerController.getBroker2Client().callClient(channel, request);
                    assert pushResponse != null;
                    switch (pushResponse.getCode()) {
                        case ResponseCode.SUCCESS: {
                            pushReplyResult.setPushOk(true);
                            break;
                        }
                        default: {
                            pushReplyResult.setPushOk(false);
                            pushReplyResult.setRemark("push reply message to " + senderId + "fail.");
                            log.warn("push reply message to <{}> return fail, response remark: {}", senderId, pushResponse.getRemark());
                        }
                    }
                } catch (RemotingException | InterruptedException e) {
                    pushReplyResult.setPushOk(false);
                    pushReplyResult.setRemark("push reply message to " + senderId + "fail.");
                    log.warn("push reply message to <{}> fail. {}", senderId, channel, e);
                }
            } else {
                pushReplyResult.setPushOk(false);
                pushReplyResult.setRemark("push reply message fail, channel of <" + senderId + "> not found.");
                log.warn(pushReplyResult.getRemark());
            }
        } else {
            log.warn(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT + " is null, can not reply message");
            pushReplyResult.setPushOk(false);
            pushReplyResult.setRemark("reply message properties[" + MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT + "] is null");
        }
        return pushReplyResult;
    }

    private void handlePushReplyResult(PushReplyResult pushReplyResult, final RemotingCommand response,
        final SendMessageResponseHeader responseHeader, int queueIdInt) {

        if (!pushReplyResult.isPushOk()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(pushReplyResult.getRemark());
        } else {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            //set to zore to avoid client decoding exception
            responseHeader.setMsgId("0");
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(0L);
        }
    }

    private void handlePutMessageResult(PutMessageResult putMessageResult,
        final RemotingCommand request, final MessageExt msg,
        final SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext,
        int queueIdInt) {
        if (putMessageResult == null) {
            log.warn("process reply message, store putMessage return null");
            return;
        }
        boolean putOk = false;

        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                putOk = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                log.info("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
                log.info(
                    "the message is illegal, maybe msg properties length limit 32k.");
                break;
            case PROPERTIES_SIZE_EXCEEDED:
                log.info(
                    "the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k.");
                break;
            case SERVICE_NOT_AVAILABLE:
                log.info(
                    "service not available now, maybe disk full, maybe your broker machine memory too small.");
                break;
            case OS_PAGECACHE_BUSY:
                log.info("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR:
                log.info("UNKNOWN_ERROR");
                break;
            default:
                log.info("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (putOk) {
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        } else {
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
    }

    class PushReplyResult {
        boolean pushOk;
        String remark;

        public PushReplyResult(boolean pushOk) {
            this.pushOk = pushOk;
            remark = "";
        }

        public boolean isPushOk() {
            return pushOk;
        }

        public void setPushOk(boolean pushOk) {
            this.pushOk = pushOk;
        }

        public String getRemark() {
            return remark;
        }

        public void setRemark(String remark) {
            this.remark = remark;
        }
    }
}
