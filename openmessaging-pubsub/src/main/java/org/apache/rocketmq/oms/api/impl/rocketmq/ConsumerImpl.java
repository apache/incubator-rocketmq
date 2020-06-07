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

import io.openmessaging.api.Action;
import io.openmessaging.api.ConsumeContext;
import io.openmessaging.api.Consumer;
import io.openmessaging.api.Message;
import io.openmessaging.api.MessageListener;
import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.oms.api.Constants;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.PropertyValueConst;

public class ConsumerImpl extends OMSConsumerAbstract implements Consumer {
    private final ConcurrentHashMap<String, MessageListener> subscribeTable = new ConcurrentHashMap<String, MessageListener>();

    public ConsumerImpl(final Properties properties) {
        super(properties);
        boolean postSubscriptionWhenPull = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.POST_SUBSCRIPTION_WHEN_PULL, "false"));
        this.defaultMQPushConsumer.setPostSubscriptionWhenPull(postSubscriptionWhenPull);

        String messageModel = properties.getProperty(PropertyKeyConst.MESSAGE_MODEL, PropertyValueConst.CLUSTERING);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.valueOf(messageModel));
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerImpl());
        super.start();
    }


    @Override
    public void subscribe(String topic, String subExpression, MessageListener listener) {
        if (null == topic) {
            throw new OMSRuntimeException("topic is null");
        }

        if (null == listener) {
            throw new OMSRuntimeException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, subExpression);
    }

    @Override
    public void subscribe(final String topic, final MessageSelector selector, final MessageListener listener) {
        if (null == topic) {
            throw new OMSRuntimeException("topic is null");
        }

        if (null == listener) {
            throw new OMSRuntimeException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, selector);
    }


    @Override
    public void unsubscribe(String topic) {
        if (null != topic) {
            this.subscribeTable.remove(topic);
            super.unsubscribe(topic);
        }
    }


    class MessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgsRMQList,
            ConsumeConcurrentlyContext contextRMQ) {
            MessageExt msgRMQ = msgsRMQList.get(0);
            Message msg = OMSUtil.msgConvert(msgRMQ);
            Map<String, String> stringStringMap = msgRMQ.getProperties();
            msg.setMsgID(msgRMQ.getMsgId());
            if (stringStringMap != null && stringStringMap.get(Constants.TRANSACTION_ID) != null) {
                msg.setMsgID(stringStringMap.get(Constants.TRANSACTION_ID));
            }
            MessageListener listener = ConsumerImpl.this.subscribeTable.get(msg.getTopic());
            if (null == listener) {
                throw new OMSRuntimeException("MessageListener is null");
            }

            final ConsumeContext context = new ConsumeContext();
            Action action = listener.consume(msg, context);
            if (action != null) {
                switch (action) {
                    case CommitMessage:
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    case ReconsumeLater:
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    default:
                        break;
                }
            }

            return null;
        }
    }
}
