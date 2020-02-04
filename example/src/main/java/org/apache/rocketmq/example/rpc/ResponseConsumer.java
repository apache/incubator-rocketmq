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

package org.apache.rocketmq.example.rpc;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ResponseConsumer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        String producerGroup = "please_rename_unique_group_name";
        String consumerGroup = "please_rename_unique_group_name";
        String topic = "RequestTopic";

        // create a producer to send reply message
        DefaultMQProducer replyProducer = new DefaultMQProducer(producerGroup);
        replyProducer.start();

        // create consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // recommend client configs
        consumer.setPullTimeDelayMillsWhenException(0L);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                for (MessageExt msg : msgs) {
                    try {
                        System.out.printf("handle message: %s", msg.toString());
                        String replyTo = MessageUtil.getReplyToClient(msg);
                        byte[] replyContent = "reply message contents.".getBytes();
                        // create reply message with given util, do not create reply message by yourself
                        Message replyMessage = MessageUtil.createReplyMessage(msg, replyContent);

                        // send reply message with producer
                        SendResult replyResult = replyProducer.send(replyMessage, 3000);
                        System.out.printf("reply to %s , %s %n", replyTo, replyResult.toString());
                    } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.subscribe(topic, "*");
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
