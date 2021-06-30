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
package org.apache.rocketmq.client.producer.selector;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectMessageQueueByMachineRoomNearBTest {

    private String topic = "FooBar";

    private final SelectMessageQueueByMachineRoomNearBy selector = new SelectMessageQueueByMachineRoomNearBy(new SelectMessageQueueByMachineRoomNearBy.MachineRoomResolver() {
        @Override
        public String brokerDeployIn(MessageQueue messageQueue) {
            return messageQueue.getBrokerName().split("-")[0];
        }

        @Override
        public String producerDeployIn() {
            return "IDC1";
        }
    });

    @Test
    public void testSelectNearByNormal() throws Exception {
        Message message = new Message(topic, new byte[] {});

        List<MessageQueue> messageQueues = new ArrayList<>();
        messageQueues.addAll(createMessageQueueList("IDC1",3));
        messageQueues.addAll(createMessageQueueList("IDC2",3));
        messageQueues.addAll(createMessageQueueList("IDC3",3));

        String orderId = "123";

        //select for 100times, only IDC1's queue should be selected
        for (int i = 0; i< 100; i++) {
            MessageQueue selected = selector.select(messageQueues, message, orderId);
            assertThat(selected.getBrokerName()).contains("IDC1");
        }

    }


    @Test
    public void testSelectNearByNearByMachineRoomOffline() throws Exception {
        Message message = new Message(topic, new byte[] {});

        List<MessageQueue> messageQueues = new ArrayList<>();
        messageQueues.addAll(createMessageQueueList("IDC2",3));
        messageQueues.addAll(createMessageQueueList("IDC3",3));
        String orderId = "123";

        //IDC1's broker is offline, other IDC's queue should be selected
        for (int i = 0; i< 100; i++) {
            MessageQueue selected = selector.select(messageQueues, message, orderId);
            assertThat(selected).isNotNull();
        }

    }

    private List<MessageQueue> createMessageQueueList(String machineRoom, int size) {
        List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>(size);
        for (int i = 0; i < size; i++) {
            MessageQueue mq = new MessageQueue(topic, machineRoom+"-brokerName", i);
            messageQueueList.add(mq);
        }
        return messageQueueList;
    }

}