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
package org.apache.rocketmq.example.stage;

import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Scenario: 5 merchants and N buyers jointly participate in 3 activities, and rewards for different activities are different.
 * call {@link StagedConcurrentlyConsumer#main(String[])} first, then call {@link Producer#main(String[])}.
 */
public class Producer {
    public static void main(String[] args) {
        try {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "localhost:9876");
            MQProducer producer = new DefaultMQProducer("please_rename_unique_group_name_4");
            producer.start();

            for (int i = 0; i < 2000; i++) {
                Message msg =
                    new Message("TopicTest", "TagA", "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                msg.putUserProperty("activityId", String.valueOf(i % 3));
                msg.putUserProperty("sellerId", String.valueOf(i % 5));
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 0);
                System.out.printf("%s\n", sendResult);
            }

            producer.shutdown();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
