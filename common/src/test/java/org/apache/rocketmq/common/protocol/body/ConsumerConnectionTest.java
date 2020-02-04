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

package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerConnectionTest {

    @Test
    public void testFromJson() {
        ConsumerConnection consumerConnection = new ConsumerConnection();
        HashSet<Connection> connections = new HashSet<Connection>();
        Connection conn = new Connection();
        connections.add(conn);

        ConcurrentHashMap<String/* Topic */, SubscriptionData> subscriptionTable = new ConcurrentHashMap<String, SubscriptionData>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionTable.put("topicA", subscriptionData);

        ConsumeType consumeType = ConsumeType.CONSUME_ACTIVELY;
        MessageModel messageModel = MessageModel.CLUSTERING;
        ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;

        consumerConnection.setConnectionSet(connections);
        consumerConnection.setSubscriptionTable(subscriptionTable);
        consumerConnection.setConsumeType(consumeType);
        consumerConnection.setMessageModel(messageModel);
        consumerConnection.setConsumeFromWhere(consumeFromWhere);

        String json = RemotingSerializable.toJson(consumerConnection, true);
        ConsumerConnection fromJson = RemotingSerializable.fromJson(json, ConsumerConnection.class);
        assertThat(fromJson.getConsumeType()).isEqualTo(ConsumeType.CONSUME_ACTIVELY);
        assertThat(fromJson.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);

        HashSet<Connection> connectionSet = fromJson.getConnectionSet();
        assertThat(connectionSet).isInstanceOf(Set.class);

        SubscriptionData data = fromJson.getSubscriptionTable().get("topicA");
        assertThat(data).isExactlyInstanceOf(SubscriptionData.class);
    }

    @Test
    public void testComputeMinVersion() {
        ConsumerConnection consumerConnection = new ConsumerConnection();
        HashSet<Connection> connections = new HashSet<Connection>();
        Connection conn1 = new Connection();
        conn1.setVersion(1);
        connections.add(conn1);
        Connection conn2 = new Connection();
        conn2.setVersion(10);
        connections.add(conn2);
        consumerConnection.setConnectionSet(connections);

        int version = consumerConnection.computeMinVersion();
        assertThat(version).isEqualTo(1);
    }
}
