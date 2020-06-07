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

package org.apache.rocketmq.oms.api.impl.rocketmq;

import io.openmessaging.api.Message;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.exception.OMSRuntimeException;
import io.openmessaging.api.order.OrderProducer;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class OrderProducerImpl extends OMSClientAbstract implements OrderProducer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final DefaultMQProducer defaultMQProducer;

    public OrderProducerImpl(final Properties properties) {
        super(properties);
        String producerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = "__ONS_PRODUCER_DEFAULT_GROUP";
        }

        String aclEnable = properties.getProperty(PropertyKeyConst.ACL_ENABLE);
        if (!UtilAll.isBlank(aclEnable) && (Boolean.parseBoolean(aclEnable))) {
            this.defaultMQProducer =
                new DefaultMQProducer(this.getNamespace(), producerGroup, new AclClientRPCHook(sessionCredentials));
        } else {
            this.defaultMQProducer = new DefaultMQProducer(this.getNamespace(), producerGroup);
        }

        this.defaultMQProducer.setProducerGroup(producerGroup);

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.VIP_CHANNEL_ENABLED, "false"));
        this.defaultMQProducer.setVipChannelEnabled(isVipChannelEnabled);

        String sendMsgTimeoutMillis = properties.getProperty(PropertyKeyConst.SEND_MSG_TIMEOUT_MILLIS, "3000");
        this.defaultMQProducer.setSendMsgTimeout(Integer.parseInt(sendMsgTimeoutMillis));

        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.defaultMQProducer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.INSTANCE_NAME, this.buildIntanceName());
        this.defaultMQProducer.setInstanceName(instanceName);
        this.defaultMQProducer.setNamesrvAddr(this.getNameServerAddr());
        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MSG_TRACE_SWITCH);
        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            LOGGER.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                String traceTopicName = properties.getProperty(PropertyKeyConst.TRACE_TOPIC_NAME);
                if (!UtilAll.isBlank(aclEnable) && (Boolean.parseBoolean(aclEnable))) {
                    this.traceDispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, traceTopicName, new AclClientRPCHook(sessionCredentials));
                } else {
                    this.traceDispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, traceTopicName, null);
                }

                ((AsyncTraceDispatcher) this.traceDispatcher).setNameServer(this.getNameServerAddr());
                ((AsyncTraceDispatcher) this.traceDispatcher).setHostProducer(defaultMQProducer.getDefaultMQProducerImpl());
                String accessChannelConfig = properties.getProperty(PropertyKeyConst.ACCESS_CHANNEL);
                if (!StringUtils.isBlank(accessChannelConfig)) {
                    AccessChannel accessChannel = AccessChannel.valueOf(accessChannelConfig);
                    ((AsyncTraceDispatcher) this.traceDispatcher).setAccessChannel(accessChannel);
                }
                this.defaultMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void start() {
        try {
            if (started.compareAndSet(false, true)) {
                this.defaultMQProducer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new OMSRuntimeException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final String shardingKey) {
        if (UtilAll.isBlank(shardingKey)) {
            throw new OMSRuntimeException("\'shardingKey\' is blank.");
        }
        message.setShardingKey(shardingKey);
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        final org.apache.rocketmq.common.message.Message msgRMQ = OMSUtil.msgConvert(message);
        try {
            org.apache.rocketmq.client.producer.SendResult sendResultRMQ =
                this.defaultMQProducer.send(msgRMQ, new org.apache.rocketmq.client.producer.MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, org.apache.rocketmq.common.message.Message msg,
                        Object shardingKey) {
                        int select = Math.abs(shardingKey.hashCode());
                        if (select < 0) {
                            select = 0;
                        }
                        return mqs.get(select % mqs.size());
                    }
                }, shardingKey);
            message.setMsgID(sendResultRMQ.getMsgId());
            SendResult sendResult = new SendResult();
            sendResult.setTopic(message.getTopic());
            sendResult.setMessageId(sendResultRMQ.getMsgId());
            return sendResult;
        } catch (Exception e) {
            throw new OMSRuntimeException("defaultMQProducer send order exception", e);
        }
    }
}
