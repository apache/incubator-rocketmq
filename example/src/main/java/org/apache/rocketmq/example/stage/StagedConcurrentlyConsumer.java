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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeStagedConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListenerStagedConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Scenario: 5 merchants and N buyers jointly participate in 3 activities, and rewards for different activities are different.
 * call {@link StagedConcurrentlyConsumer#main(String[])} first, then call {@link Producer#main(String[])}.
 * Below are more examples of how to use it:
 *
 * @see org.apache.rocketmq.client.impl.consumer.ConsumeMessageStagedConcurrentlyServiceTest
 */
public class StagedConcurrentlyConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("TopicTest", "TagA");
        consumer.registerMessageListener(new MessageListenerStagedConcurrently() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeStagedConcurrentlyContext context) {
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    // The stageIndex increases from 0. The "stages" represented by each stageIndex are in order,
                    // and the "stages" are out of order. When the last stage is reached, the stageIndex is -1.
                    // You can see that MessageListenerOrderly is the same as the order. Order for each queue (partition)
                    System.out.printf("consumeThread=%s\tstageIndex=%s\tqueueId=%s\tcontent:%s\n",
                        Thread.currentThread().getName(), context.getStageIndex(), msg.getQueueId(), new String(msg.getBody()));
                }

                try {
                    // Simulating business logic processing...
                    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }

            @Override
            public Map<String, List<Integer>> getStageDefinitionStrategies() {
                List<Integer> list = new ArrayList<>();
                for (int i = 1; i <= 50; i++) {
                    list.add(i);
                }
                Map<String, List<Integer>> map = new HashMap<>(1);
                map.put("1", list);
                map.put("2", Arrays.asList(10, 20));
                map.put("3", Collections.singletonList(100));
                return map;
            }

            @Override
            public String computeStrategy(MessageExt message) {
                return message.getProperty("activityId");
            }

            @Override
            public String computeGroup(MessageExt message) {
                return message.getProperty("sellerId");
            }

            @Override
            public void rollbackCurrentStageOffsetIfNeed(String topic, String strategyId,
                String groupId, AtomicInteger currentStageOffset, List<MessageExt> msgs) {
                if ("TopicTest".equals(topic) && currentStageOffset.get() >= 399) {
                    currentStageOffset.set(0);
                }
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
