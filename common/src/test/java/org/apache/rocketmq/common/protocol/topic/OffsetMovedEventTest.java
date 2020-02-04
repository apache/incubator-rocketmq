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

package org.apache.rocketmq.common.protocol.topic;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

public class OffsetMovedEventTest {

  @Test
  public void testFromJson() throws Exception {
    OffsetMovedEvent event = mockOffsetMovedEvent();

    String json = event.toJson();
    OffsetMovedEvent fromJson = RemotingSerializable.fromJson(json, OffsetMovedEvent.class);

    assertEquals(event, fromJson);
  }

  @Test
  public void testFromBytes() throws Exception {
    OffsetMovedEvent event = mockOffsetMovedEvent();

    byte[] encodeData = event.encode();
    OffsetMovedEvent decodeData = RemotingSerializable.decode(encodeData, OffsetMovedEvent.class);

    assertEquals(event, decodeData);
  }

  private void assertEquals(OffsetMovedEvent srcData, OffsetMovedEvent decodeData) {
    assertThat(decodeData.getConsumerGroup()).isEqualTo(srcData.getConsumerGroup());
    assertThat(decodeData.getMessageQueue().getTopic())
        .isEqualTo(srcData.getMessageQueue().getTopic());
    assertThat(decodeData.getMessageQueue().getBrokerName())
        .isEqualTo(srcData.getMessageQueue().getBrokerName());
    assertThat(decodeData.getMessageQueue().getQueueId())
        .isEqualTo(srcData.getMessageQueue().getQueueId());
    assertThat(decodeData.getOffsetRequest()).isEqualTo(srcData.getOffsetRequest());
    assertThat(decodeData.getOffsetNew()).isEqualTo(srcData.getOffsetNew());
  }

  private OffsetMovedEvent mockOffsetMovedEvent() {
    OffsetMovedEvent event = new OffsetMovedEvent();
    event.setConsumerGroup("test-group");
    event.setMessageQueue(new MessageQueue("test-topic", "test-broker", 0));
    event.setOffsetRequest(3000L);
    event.setOffsetNew(1000L);
    return event;
  }
}
