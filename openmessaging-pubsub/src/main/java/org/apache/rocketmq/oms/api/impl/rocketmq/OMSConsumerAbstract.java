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

import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class OMSConsumerAbstract extends OMSClientAbstract {
    final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    protected final DefaultMQPushConsumer defaultMQPushConsumer;
    private final static int MAX_CACHED_MESSAGE_SIZE_IN_MIB = 2048;
    private final static int MIN_CACHED_MESSAGE_SIZE_IN_MIB = 16;
    private final static int MAX_CACHED_MESSAGE_AMOUNT = 50000;
    private final static int MIN_CACHED_MESSAGE_AMOUNT = 100;

    private int maxCachedMessageSizeInMiB = 512;

    private int maxCachedMessageAmount = 5000;

    public OMSConsumerAbstract(final Properties properties) {
        super(properties);

        String consumerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(consumerGroup)) {
            throw new OMSRuntimeException("ConsumerId property is null");
        }

        String aclEnable = properties.getProperty(PropertyKeyConst.ACL_ENABLE);
        if (!UtilAll.isBlank(aclEnable) && (Boolean.parseBoolean(aclEnable))) {
            this.defaultMQPushConsumer =
                new DefaultMQPushConsumer(this.getNamespace(), consumerGroup, new AclClientRPCHook(sessionCredentials));
        } else {
            this.defaultMQPushConsumer = new DefaultMQPushConsumer(this.getNamespace(), consumerGroup);
        }

        String maxReconsumeTimes = properties.getProperty(PropertyKeyConst.MAX_RECONSUME_TIMES);
        if (!UtilAll.isBlank(maxReconsumeTimes)) {
            try {
                this.defaultMQPushConsumer.setMaxReconsumeTimes(Integer.parseInt(maxReconsumeTimes));
            } catch (NumberFormatException ignored) {
            }
        }

        String maxBatchMessageCount = properties.getProperty(PropertyKeyConst.MAX_BATCH_MESSAGE_COUNT);
        if (!UtilAll.isBlank(maxBatchMessageCount)) {
            this.defaultMQPushConsumer.setPullBatchSize(Integer.valueOf(maxBatchMessageCount));
        }

        String consumeTimeout = properties.getProperty(PropertyKeyConst.CONSUME_TIMEOUT);
        if (!UtilAll.isBlank(consumeTimeout)) {
            try {
                this.defaultMQPushConsumer.setConsumeTimeout(Integer.parseInt(consumeTimeout));
            } catch (NumberFormatException ignored) {
            }
        }

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.VIP_CHANNEL_ENABLED, "false"));
        this.defaultMQPushConsumer.setVipChannelEnabled(isVipChannelEnabled);
        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.defaultMQPushConsumer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.INSTANCE_NAME, this.buildIntanceName());
        this.defaultMQPushConsumer.setInstanceName(instanceName);
        this.defaultMQPushConsumer.setNamesrvAddr(this.getNameServerAddr());

        String consumeThreadNums = properties.getProperty(PropertyKeyConst.CONSUME_THREAD_NUMS);
        if (!UtilAll.isBlank(consumeThreadNums)) {
            this.defaultMQPushConsumer.setConsumeThreadMin(Integer.valueOf(consumeThreadNums));
            this.defaultMQPushConsumer.setConsumeThreadMax(Integer.valueOf(consumeThreadNums));
        }

        String configuredCachedMessageAmount = properties.getProperty(PropertyKeyConst.MAX_CACHED_MESSAGE_AMOUNT);
        if (!UtilAll.isBlank(configuredCachedMessageAmount)) {
            maxCachedMessageAmount = Math.min(MAX_CACHED_MESSAGE_AMOUNT, Integer.valueOf(configuredCachedMessageAmount));
            maxCachedMessageAmount = Math.max(MIN_CACHED_MESSAGE_AMOUNT, maxCachedMessageAmount);
            this.defaultMQPushConsumer.setPullThresholdForTopic(maxCachedMessageAmount);

        }

        String configuredCachedMessageSizeInMiB = properties.getProperty(PropertyKeyConst.MAX_CACHED_MESSAGE_SIZE_IN_MB);
        if (!UtilAll.isBlank(configuredCachedMessageSizeInMiB)) {
            maxCachedMessageSizeInMiB = Math.min(MAX_CACHED_MESSAGE_SIZE_IN_MIB, Integer.valueOf(configuredCachedMessageSizeInMiB));
            maxCachedMessageSizeInMiB = Math.max(MIN_CACHED_MESSAGE_SIZE_IN_MIB, maxCachedMessageSizeInMiB);
            this.defaultMQPushConsumer.setPullThresholdSizeForTopic(maxCachedMessageSizeInMiB);
        }

        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MSG_TRACE_SWITCH);

        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            LOGGER.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                String traceTopicName = properties.getProperty(PropertyKeyConst.TRACE_TOPIC_NAME);
                if (!UtilAll.isBlank(aclEnable) && (Boolean.parseBoolean(aclEnable))) {
                    this.traceDispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, traceTopicName, new AclClientRPCHook(sessionCredentials));
                } else {
                    this.traceDispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, traceTopicName, null);
                }
                ((AsyncTraceDispatcher) this.traceDispatcher).setNameServer(this.getNameServerAddr());
                ((AsyncTraceDispatcher) this.traceDispatcher).setHostConsumer(defaultMQPushConsumer.getDefaultMQPushConsumerImpl());
                String accessChannelConfig = properties.getProperty(PropertyKeyConst.ACCESS_CHANNEL);
                if (!StringUtils.isBlank(accessChannelConfig)) {
                    AccessChannel accessChannel = AccessChannel.valueOf(accessChannelConfig);
                    ((AsyncTraceDispatcher) this.traceDispatcher).setAccessChannel(accessChannel);
                }
                this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(new ConsumeMessageTraceHookImpl(traceDispatcher));

            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    protected void subscribe(String topic, String subExpression) {
        try {
            this.defaultMQPushConsumer.subscribe(topic, subExpression);
        } catch (MQClientException e) {
            throw new OMSRuntimeException("defaultMQPushConsumer subscribe exception", e);
        }
    }

    protected void subscribe(final String topic, final MessageSelector selector) {
        String subExpression = "*";
        String type = org.apache.rocketmq.common.filter.ExpressionType.TAG;
        if (selector != null) {
            if (selector.getType() == null) {
                throw new OMSRuntimeException("Expression type is null!");
            }
            subExpression = selector.getSubExpression();
            type = selector.getType().name();
        }

        org.apache.rocketmq.client.consumer.MessageSelector messageSelector;
        if (org.apache.rocketmq.common.filter.ExpressionType.SQL92.equals(type)) {
            messageSelector = org.apache.rocketmq.client.consumer.MessageSelector.bySql(subExpression);
        } else if (org.apache.rocketmq.common.filter.ExpressionType.TAG.equals(type)) {
            messageSelector = org.apache.rocketmq.client.consumer.MessageSelector.byTag(subExpression);
        } else {
            throw new OMSRuntimeException(String.format("Expression type %s is unknown!", type));
        }

        try {
            this.defaultMQPushConsumer.subscribe(topic, messageSelector);
        } catch (MQClientException e) {
            throw new OMSRuntimeException("Consumer subscribe exception", e);
        }
    }

    protected void unsubscribe(String topic) {
        this.defaultMQPushConsumer.unsubscribe(topic);
    }

    @Override
    public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.defaultMQPushConsumer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new OMSRuntimeException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.defaultMQPushConsumer.shutdown();
        }
        super.shutdown();
    }
}
