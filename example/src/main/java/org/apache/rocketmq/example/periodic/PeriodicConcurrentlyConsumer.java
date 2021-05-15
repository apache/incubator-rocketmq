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
package org.apache.rocketmq.example.periodic;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerPeriodicConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * call {@link PeriodicConcurrentlyConsumer#main(java.lang.String[])} first,
 * then call {@link Producer#main(java.lang.String[])}
 */
public class PeriodicConcurrentlyConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTest", "TagA");
        consumer.registerMessageListener(new MessageListenerPeriodicConcurrently() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context, int stageIndex) {
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    // stageIndex从0开始递增，每个stageIndex代表的"阶段"之间是有序的，
                    // 而"阶段"内部是乱序的，当到达最后一个阶段时，stageIndex为-1
                    // 可以看到MessageListenerOrderly和一样, 订单对每个queue(分区)有序
                    System.out.println("consumeThread=" + Thread.currentThread().getName() +", stageIndex="+stageIndex+ ", queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                }

                try {
                    //模拟业务逻辑处理中...
                    TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }

            @Override
            public List<Integer> getStageDefinitions() {
                List<Integer> list = new ArrayList<>();
                for (int i = 1; i <= 50; i++) {
                    list.add(i);
                }
                return list;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

}
