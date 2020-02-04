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

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ResetOffsetBodyTest {

    @Test
    public void testFromJson() throws Exception {
        ResetOffsetBody rob = new ResetOffsetBody();
        Map<MessageQueue, Long> offsetMap = new HashMap<MessageQueue, Long>();
        MessageQueue queue = new MessageQueue();
        queue.setQueueId(1);
        queue.setBrokerName("brokerName");
        queue.setTopic("topic");
        offsetMap.put(queue, 100L);
        rob.setOffsetTable(offsetMap);
        String json = RemotingSerializable.toJson(rob, true);
        ResetOffsetBody fromJson = RemotingSerializable.fromJson(json, ResetOffsetBody.class);
        assertThat(fromJson.getOffsetTable().get(queue)).isEqualTo(100L);
        assertThat(fromJson.getOffsetTable().size()).isEqualTo(1);
    }
}
