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
import io.openmessaging.api.Message;
import io.openmessaging.api.batch.BatchConsumer;
import io.openmessaging.api.batch.BatchMessageListener;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.oms.api.Constants;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.PropertyValueConst;

public class BatchConsumerImpl extends OMSConsumerAbstract implements BatchConsumer {
    private final static int MAX_BATCH_SIZE = 32;
    private final static int MIN_BATCH_SIZE = 1;
    private final ConcurrentHashMap<String, BatchMessageListener> subscribeTable = new ConcurrentHashMap<String, BatchMessageListener>();

    public BatchConsumerImpl(final Properties properties) {
        super(properties);

        boolean postSubscriptionWhenPull = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.POST_SUBSCRIPTION_WHEN_PULL, "false"));
        this.defaultMQPushConsumer.setPostSubscriptionWhenPull(postSubscriptionWhenPull);

        String messageModel = properties.getProperty(PropertyKeyConst.MESSAGE_MODEL, PropertyValueConst.CLUSTERING);
        this.defaultMQPushConsumer.setMessageModel(MessageModel.valueOf(messageModel));

        String consumeBatchSize = properties.getProperty(PropertyKeyConst.CONSUME_MESSAGE_BATCH_MAX_SIZE);
        if (!UtilAll.isBlank(consumeBatchSize)) {
            int batchSize = Math.min(MAX_BATCH_SIZE, Integer.valueOf(consumeBatchSize));
            batchSize = Math.max(MIN_BATCH_SIZE, batchSize);
            this.defaultMQPushConsumer.setConsumeMessageBatchMaxSize(batchSize);
        }
    }

    @Override
    public void start() {
        this.defaultMQPushConsumer.registerMessageListener(new BatchMessageListenerImpl());
        super.start();
    }

    @Override
    public void subscribe(String topic, String subExpression, BatchMessageListener listener) {
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
    public void unsubscribe(String topic) {
        if (null != topic) {
            this.subscribeTable.remove(topic);
            super.unsubscribe(topic);
        }
    }

    class BatchMessageListenerImpl implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
            ConsumeConcurrentlyContext contextRMQ) {
            List<Message> msgList = new ArrayList<Message>();
            for (MessageExt rmqMsg : rmqMsgList) {
                Message msg = OMSUtil.msgConvert(rmqMsg);
                Map<String, String> propertiesMap = rmqMsg.getProperties();
                msg.setMsgID(rmqMsg.getMsgId());
                if (propertiesMap != null && propertiesMap.get(Constants.TRANSACTION_ID) != null) {
                    msg.setMsgID(propertiesMap.get(Constants.TRANSACTION_ID));
                }
                msgList.add(msg);
            }

            BatchMessageListener listener = BatchConsumerImpl.this.subscribeTable.get(msgList.get(0).getTopic());
            if (null == listener) {
                throw new OMSRuntimeException("BatchMessageListener is null");
            }

            final ConsumeContext context = new ConsumeContext();
            Action action = listener.consume(msgList, context);
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
