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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.factory;

import java.util.UUID;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.test.client.rmq.RMQBroadCastConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQSqlConsumer;
import org.apache.rocketmq.test.listener.AbstractListener;

public class ConsumerFactory {

    public static RMQNormalConsumer getRMQNormalConsumer(String nsAddr, String consumerGroup,
        String topic, String subExpression,
        AbstractListener listener) {
        return getRMQNormalConsumer(nsAddr, consumerGroup, topic, subExpression, listener, false);
    }

    public static RMQNormalConsumer getRMQNormalConsumer(String nsAddr, String consumerGroup,
        String topic, String subExpression,
        AbstractListener listener, boolean useTLS) {
        RMQNormalConsumer consumer = new RMQNormalConsumer(nsAddr, topic, subExpression,
            consumerGroup, listener);
        consumer.create(useTLS);
        consumer.start();
        return consumer;
    }

    public static RMQBroadCastConsumer getRMQBroadCastConsumer(String nsAddr, String consumerGroup,
        String topic, String subExpression,
        AbstractListener listner) {
        RMQBroadCastConsumer consumer = new RMQBroadCastConsumer(nsAddr, topic, subExpression,
            consumerGroup, listner);
        consumer.create();
        consumer.start();
        return consumer;
    }

    public static RMQSqlConsumer getRMQSqlConsumer(String nsAddr, String consumerGroup,
        String topic, MessageSelector selector,
        AbstractListener listner) {
        RMQSqlConsumer consumer = new RMQSqlConsumer(nsAddr, topic, selector,
            consumerGroup, listner);
        consumer.create();
        consumer.start();
        return consumer;
    }

    public static DefaultMQPullConsumer getRMQPullConsumer(String nsAddr, String consumerGroup) throws Exception {
        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(consumerGroup);
        defaultMQPullConsumer.setInstanceName(UUID.randomUUID().toString());
        defaultMQPullConsumer.setNamesrvAddr(nsAddr);
        defaultMQPullConsumer.start();
        return defaultMQPullConsumer;
    }
}
