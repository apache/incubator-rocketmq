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
package io.openmessaging.rocketmq.utils;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.extension.QueueMetaData;
import io.openmessaging.message.Header;
import io.openmessaging.producer.SendResult;
import io.openmessaging.rocketmq.domain.DefaultQueueMetaData;
import io.openmessaging.rocketmq.domain.BytesMessageImpl;
import io.openmessaging.rocketmq.domain.RocketMQConstants;
import io.openmessaging.rocketmq.domain.SendResultImpl;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageQueue;

public class OMSClientUtil {

    /**
     * Builds a OMS client instance name.
     *
     * @return a unique instance name
     */
    public static String buildInstanceName() {
        return Integer.toString(UtilAll.getPid()) + "%OpenMessaging" + "%" + System.nanoTime();
    }

    public static org.apache.rocketmq.common.message.Message msgConvert(BytesMessageImpl omsMessage) {
        org.apache.rocketmq.common.message.Message rmqMessage = new org.apache.rocketmq.common.message.Message();
        rmqMessage.setBody(omsMessage.getData());

        Header sysHeaders = omsMessage.header();
        KeyValue userHeaders = omsMessage.properties();

        //All destinations in RocketMQ use Topic
        rmqMessage.setTopic(sysHeaders.getDestination());

        long deliverTime = sysHeaders.getBornTimestamp();
        if (deliverTime > 0) {
            rmqMessage.putUserProperty(RocketMQConstants.START_DELIVER_TIME, String.valueOf(deliverTime));
        }

        for (String key : userHeaders.keySet()) {
            MessageAccessor.putProperty(rmqMessage, key, userHeaders.getString(key));
        }

        MessageAccessor.putProperty(rmqMessage, RocketMQConstants.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(sysHeaders.getDeliveryCount()));
        return rmqMessage;
    }

    public static BytesMessageImpl msgConvert(org.apache.rocketmq.common.message.MessageExt rmqMsg) {
        BytesMessageImpl omsMsg = new BytesMessageImpl();
        omsMsg.setData(rmqMsg.getBody());

        final Set<Map.Entry<String, String>> entries = rmqMsg.getProperties().entrySet();

        for (final Map.Entry<String, String> entry : entries) {
            if (!isOMSHeader(entry.getKey())) {
                omsMsg.properties().put(entry.getKey(), entry.getValue());
            }
        }

        omsMsg.header().setMessageId(rmqMsg.getMsgId());
        omsMsg.header().setDestination(rmqMsg.getTopic());
        omsMsg.header().setBornHost(String.valueOf(rmqMsg.getBornHost()));
        omsMsg.header().setBornTimestamp(rmqMsg.getBornTimestamp());
        omsMsg.header().setDeliveryCount(rmqMsg.getDelayTimeLevel());
        omsMsg.extensionHeader().setPartition(rmqMsg.getQueueId());
        omsMsg.extensionHeader().setOffset(rmqMsg.getQueueOffset());
        omsMsg.extensionHeader().setDelayTime(rmqMsg.getDelayTimeLevel());
        omsMsg.extensionHeader().setMessageKey(rmqMsg.getKeys());
        omsMsg.extensionHeader().setStoreHost(rmqMsg.getStoreHost().toString());
        omsMsg.extensionHeader().setStoreTimestamp(rmqMsg.getStoreTimestamp());
        omsMsg.extensionHeader().setTransactionId(rmqMsg.getTransactionId());

        return omsMsg;
    }

    public static boolean isOMSHeader(String value) {
        for (Field field : Header.class.getDeclaredFields()) {
            try {
                if (field.get(Header.class).equals(value)) {
                    return true;
                }
            } catch (IllegalAccessException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Convert a RocketMQ SEND_OK SendResult instance to a OMS SendResult.
     */
    public static SendResult sendResultConvert(org.apache.rocketmq.client.producer.SendResult rmqResult) {
        assert rmqResult.getSendStatus().equals(SendStatus.SEND_OK);
        return new SendResultImpl(rmqResult.getMsgId(), OMS.newKeyValue());
    }

    public static KeyValue buildKeyValue(KeyValue... keyValues) {
        KeyValue keyValue = OMS.newKeyValue();
        for (KeyValue properties : keyValues) {
            for (String key : properties.keySet()) {
                keyValue.put(key, properties.getString(key));
            }
        }
        return keyValue;
    }

    public static Set<QueueMetaData> queueMetaDataConvert(Collection<MessageQueue> messageQueues) {
        Set<QueueMetaData> queueMetaDatas = new HashSet<>(32);
        if (null != messageQueues && !messageQueues.isEmpty()) {
            for (MessageQueue messageQueue : messageQueues) {
                QueueMetaData queueMetaData = new DefaultQueueMetaData(messageQueue.getTopic(), messageQueue.getQueueId());
                queueMetaDatas.add(queueMetaData);
            }
        } else {
            return null;
        }
        return queueMetaDatas;
    }
}
