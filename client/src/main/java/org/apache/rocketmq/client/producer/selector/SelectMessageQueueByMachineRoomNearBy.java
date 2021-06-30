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

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import java.util.ArrayList;
import java.util.List;


public class SelectMessageQueueByMachineRoomNearBy implements MessageQueueSelector {
    private final MessageQueueSelector inner;
    private final MachineRoomResolver machineRoomResolver;

    public SelectMessageQueueByMachineRoomNearBy(MachineRoomResolver machineRoomResolver) {
        this(machineRoomResolver, new SelectMessageQueueByRandom());
    }

    public SelectMessageQueueByMachineRoomNearBy(MachineRoomResolver machineRoomResolver, MessageQueueSelector inner) {
        this.machineRoomResolver = machineRoomResolver;
        this.inner = inner;
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        List<MessageQueue> candidateMqs = new ArrayList<>();
        for  (MessageQueue mq: mqs) {
            if (machineRoomResolver.brokerDeployIn(mq).equals(machineRoomResolver.producerDeployIn())) {
                candidateMqs.add(mq);
            }
        }

        //no nearby broker queues, use the rest anyway
        if (candidateMqs.isEmpty()) {
            candidateMqs = mqs;
        }


        return inner.select(candidateMqs, msg, arg);
    }

    /**
     * A resolver object to determine which machine room do the message queues or producers are deployed in.
     *
     * SelectMessageQueueByMachineRoomNearBy will prefer to selecting to message queues which has the same machine room name with the producer's
     *
     * The result returned from the implemented method CANNOT be null.
     */
    public interface MachineRoomResolver {
        String brokerDeployIn(MessageQueue messageQueue);

        String producerDeployIn();
    }
}
