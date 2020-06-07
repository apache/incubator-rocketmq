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
import io.openmessaging.api.transaction.LocalTransactionExecuter;
import io.openmessaging.api.transaction.TransactionProducer;
import io.openmessaging.api.transaction.TransactionStatus;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.oms.api.Constants;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class TransactionProducerImpl extends OMSClientAbstract implements TransactionProducer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    TransactionMQProducer transactionMQProducer = null;
    private Properties properties;

    public TransactionProducerImpl(Properties properties, TransactionCheckListener transactionCheckListener) {
        super(properties);
        this.properties = properties;
        String producerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = "__ONS_PRODUCER_DEFAULT_GROUP";
        }

        String aclEnable = properties.getProperty(PropertyKeyConst.ACL_ENABLE);
        if (!UtilAll.isBlank(aclEnable) && (Boolean.parseBoolean(aclEnable))) {
            transactionMQProducer =
                new TransactionMQProducer(this.getNamespace(), producerGroup, new AclClientRPCHook(sessionCredentials));
        } else {
            transactionMQProducer =
                new TransactionMQProducer(this.getNamespace(), producerGroup);
        }

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.VIP_CHANNEL_ENABLED, "false"));
        transactionMQProducer.setVipChannelEnabled(isVipChannelEnabled);
        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.transactionMQProducer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.INSTANCE_NAME, this.buildIntanceName());
        this.transactionMQProducer.setInstanceName(instanceName);

        transactionMQProducer.setTransactionCheckListener(transactionCheckListener);
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
                ((AsyncTraceDispatcher) this.traceDispatcher).setHostProducer(transactionMQProducer.getDefaultMQProducerImpl());
                String accessChannelConfig = properties.getProperty(PropertyKeyConst.ACCESS_CHANNEL);
                if (!StringUtils.isBlank(accessChannelConfig)) {
                    AccessChannel accessChannel = AccessChannel.valueOf(accessChannelConfig);
                    ((AsyncTraceDispatcher) this.traceDispatcher).setAccessChannel(accessChannel);
                }
                this.transactionMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
            }
        }
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            if (transactionMQProducer.getTransactionCheckListener() == null) {
                throw new IllegalArgumentException("TransactionCheckListener is null");
            }
            transactionMQProducer.setNamesrvAddr(this.nameServerAddr);
            try {
                transactionMQProducer.start();
                super.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.transactionMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            transactionMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final LocalTransactionExecuter executer, Object arg) {
        this.checkONSProducerServiceState(this.transactionMQProducer.getDefaultMQProducerImpl());
        org.apache.rocketmq.common.message.Message msgRMQ = OMSUtil.msgConvert(message);
        org.apache.rocketmq.client.producer.TransactionSendResult sendResultRMQ = null;
        try {
            sendResultRMQ = transactionMQProducer.sendMessageInTransaction(msgRMQ,
                new org.apache.rocketmq.client.producer.LocalTransactionExecuter() {
                    @Override
                    public LocalTransactionState executeLocalTransactionBranch(
                        org.apache.rocketmq.common.message.Message msg,
                        Object arg) {
                        String msgId = msg.getProperty(Constants.TRANSACTION_ID);
                        message.setMsgID(msgId);
                        TransactionStatus transactionStatus = executer.execute(message, arg);
                        if (TransactionStatus.CommitTransaction == transactionStatus) {
                            return LocalTransactionState.COMMIT_MESSAGE;
                        } else if (TransactionStatus.RollbackTransaction == transactionStatus) {
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                        }
                        return LocalTransactionState.UNKNOW;
                    }
                }, arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (sendResultRMQ.getLocalTransactionState() == LocalTransactionState.ROLLBACK_MESSAGE) {
            throw new RuntimeException("local transaction branch failed ,so transaction rollback");
        }
        SendResult sendResult = new SendResult();
        sendResult.setMessageId(sendResultRMQ.getMsgId());
        sendResult.setTopic(sendResultRMQ.getMessageQueue().getTopic());
        return sendResult;
    }

}
