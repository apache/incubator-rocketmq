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
import io.openmessaging.api.MessageSelector;
import io.openmessaging.api.exception.OMSRuntimeException;
import io.openmessaging.api.order.ConsumeOrderContext;
import io.openmessaging.api.order.MessageOrderListener;
import io.openmessaging.api.order.OrderAction;
import io.openmessaging.api.order.OrderConsumer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.oms.api.PropertyKeyConst;

public class OrderConsumerImpl extends OMSConsumerAbstract implements OrderConsumer {
    private final ConcurrentHashMap<String, MessageOrderListener> subscribeTable = new ConcurrentHashMap<String, MessageOrderListener>();

    public OrderConsumerImpl(final Properties properties) {
        super(properties);
        String suspendTimeMillis = properties.getProperty(PropertyKeyConst.SUSPEND_TIME_MILLIS);
        if (!UtilAll.isBlank(suspendTimeMillis)) {
            try {
                this.defaultMQPushConsumer.setSuspendCurrentQueueTimeMillis(Long.parseLong(suspendTimeMillis));
            } catch (NumberFormatException ignored) {
            }
        }
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderlyImpl());
        super.start();
    }

    @Override
    public void subscribe(String topic, String subExpression, MessageOrderListener listener) {
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
    public void subscribe(final String topic, final MessageSelector selector, final MessageOrderListener listener) {
        if (null == topic) {
            throw new OMSRuntimeException("topic is null");
        }

        if (null == listener) {
            throw new OMSRuntimeException("listener is null");
        }
        this.subscribeTable.put(topic, listener);
        super.subscribe(topic, selector);
    }

    class MessageListenerOrderlyImpl implements MessageListenerOrderly {

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> arg0, ConsumeOrderlyContext arg1) {
            MessageExt msgRMQ = arg0.get(0);
            Message msg = OMSUtil.msgConvert(msgRMQ);
            msg.setMsgID(msgRMQ.getMsgId());

            MessageOrderListener listener = OrderConsumerImpl.this.subscribeTable.get(msg.getTopic());
            if (null == listener) {
                throw new OMSRuntimeException("MessageOrderListener is null");
            }

            final ConsumeOrderContext context = new ConsumeOrderContext();
            OrderAction action = listener.consume(msg, context);
            if (action != null) {
                switch (action) {
                    case Success:
                        return ConsumeOrderlyStatus.SUCCESS;
                    case Suspend:
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    default:
                        break;
                }
            }

            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
}
