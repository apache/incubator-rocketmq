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
package org.apache.rocketmq.client.impl.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ConsumeMessageOrderlyServiceTest {
    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private DefaultMQPushConsumer pushConsumer;

    @Before
    public void init() throws Exception {
        consumerGroup = "FooBarGroup" + System.currentTimeMillis();
        pushConsumer = new DefaultMQPushConsumer(consumerGroup);
    }

    @Test
    public void testConsumeMessageDirectly_WithNoException() {
        Map<ConsumeOrderlyStatus, CMResult> map = new HashMap();
        map.put(ConsumeOrderlyStatus.SUCCESS, CMResult.CR_SUCCESS);
        map.put(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT, CMResult.CR_LATER);
        map.put(ConsumeOrderlyStatus.COMMIT, CMResult.CR_COMMIT);
        map.put(ConsumeOrderlyStatus.ROLLBACK, CMResult.CR_ROLLBACK);
        map.put(null, CMResult.CR_RETURN_NULL);

        for (ConsumeOrderlyStatus consumeOrderlyStatus : map.keySet()) {
            final ConsumeOrderlyStatus status = consumeOrderlyStatus;
            MessageListenerOrderly listenerOrderly = new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    return status;
                }
            };

            ConsumeMessageOrderlyService consumeMessageOrderlyService = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), listenerOrderly);
            MessageExt msg = new MessageExt();
            msg.setTopic(topic);
            assertTrue(consumeMessageOrderlyService.consumeMessageDirectly(msg, brokerName).getConsumeResult().equals(map.get(consumeOrderlyStatus)));
        }

    }

    @Test
    public void testConsumeMessageDirectly_WithException() {
        MessageListenerOrderly listenerOrderly = new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                throw new RuntimeException();
            }
        };

        ConsumeMessageOrderlyService consumeMessageOrderlyService = new ConsumeMessageOrderlyService(pushConsumer.getDefaultMQPushConsumerImpl(), listenerOrderly);
        MessageExt msg = new MessageExt();
        msg.setTopic(topic);
        assertTrue(consumeMessageOrderlyService.consumeMessageDirectly(msg, brokerName).getConsumeResult().equals(CMResult.CR_THROW_EXCEPTION));
    }

}
