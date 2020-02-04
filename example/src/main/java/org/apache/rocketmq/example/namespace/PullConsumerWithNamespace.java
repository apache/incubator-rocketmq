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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullConsumerWithNamespace {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws Exception {
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer("InstanceTest", "cidTest");
        pullConsumer.setNamesrvAddr("127.0.0.1:9876");
        pullConsumer.start();

        Set<MessageQueue> mqs = pullConsumer.fetchSubscribeMessageQueues("topicTest");
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the topic: %s, queue: %s%n", mq.getTopic(), mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                        pullConsumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);

                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            dealWithPullResult(pullResult);
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        pullConsumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null) {
            return offset;
        }

        return 0;
    }

    private static void dealWithPullResult(PullResult pullResult) {
        if (null == pullResult || pullResult.getMsgFoundList().isEmpty()) {
            return;
        }
        pullResult.getMsgFoundList().stream().forEach(
            (msg) -> System.out.printf("Topic is:%s, msgId is:%s%n" , msg.getTopic(), msg.getMsgId()));
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}