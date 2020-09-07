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
package org.apache.rocketmq.example.delaymessage;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Producer {
    public static void main(String[] args) throws UnsupportedEncodingException {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
            producer.setNamesrvAddr("172.26.54.66:9876");
            producer.start();

            long baseTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-09-07 10:41:00").getTime();
            int msgCounts = 20;
            int timeDuration = 10;
            for (int i = 0; i < msgCounts; i++) {
                Message msg = new Message("TopicTestDelayMsg", "tag", "key", "Hello RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
                long startDeliverTime = baseTime + (i % timeDuration) * 1000;
                msg.setStartDeliverTime(startDeliverTime);
                SendResult sendResult = producer.send(msg);

                System.out.printf("%s%n", sendResult);
            }

            producer.shutdown();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException | ParseException e) {
            e.printStackTrace();
        }
    }
}
