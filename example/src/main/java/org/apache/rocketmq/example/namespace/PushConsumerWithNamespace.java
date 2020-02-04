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
package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

public class PushConsumerWithNamespace {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("InstanceTest", "cidTest");
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQPushConsumer.subscribe("topicTest", "*");
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently)(msgs, context) -> {
            msgs.stream().forEach((msg) -> {
                System.out.printf("Msg topic is:%s, MsgId is:%s, reconsumeTimes is:%s%n", msg.getTopic() , msg.getMsgId(), msg.getReconsumeTimes());
            });
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });

        defaultMQPushConsumer.start();
    }
}