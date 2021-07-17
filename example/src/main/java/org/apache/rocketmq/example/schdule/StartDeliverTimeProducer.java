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
package org.apache.rocketmq.example.schdule;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class StartDeliverTimeProducer {
    // modify broker config messageDelayLevel=1s 2s 3s 4s 5s
    public static void main(String[] args) throws Throwable {
        DefaultMQProducer producer = new DefaultMQProducer("StartDeliverTimeProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 1000; i++) {
            long startDeliverTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L + ThreadLocalRandom.current().nextLong(10L));
            Message msg = new Message("TopicTest", (startDeliverTime + "").getBytes());
            msg.setStartDeliverTime(startDeliverTime);
            producer.send(msg);
        }
        producer.shutdown();
    }
}
