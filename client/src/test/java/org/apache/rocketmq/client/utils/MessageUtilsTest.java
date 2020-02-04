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

package org.apache.rocketmq.client.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;

public class MessageUtilsTest {

    @Test
    public void testCreateReplyMessage() throws MQClientException {
        Message msg = MessageUtil.createReplyMessage(createReplyMessage("clusterName"), new byte[] {'a'});
        assertThat(msg.getTopic()).isEqualTo("clusterName" + "_" + MixAll.REPLY_TOPIC_POSTFIX);
        assertThat(msg.getProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT)).isEqualTo("127.0.0.1");
        assertThat(msg.getProperty(MessageConst.PROPERTY_MESSAGE_TTL)).isEqualTo("3000");
    }

    @Test
    public void testCreateReplyMessage_Exception() throws MQClientException {
        try {
            Message msg = MessageUtil.createReplyMessage(createReplyMessage(null), new byte[] {'a'});
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("create reply message fail, requestMessage error, property[" + MessageConst.PROPERTY_CLUSTER + "] is null.");
        }
    }

    @Test
    public void testCreateReplyMessage_reqMsgIsNull() throws MQClientException {
        try {
            Message msg = MessageUtil.createReplyMessage(null, new byte[] {'a'});
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("create reply message fail, requestMessage cannot be null.");
        }
    }

    @Test
    public void testGetReplyToClient() throws MQClientException {
        Message msg = createReplyMessage("clusterName");
        String replyToClient = MessageUtil.getReplyToClient(msg);
        assertThat(replyToClient).isNotNull();
        assertThat(replyToClient).isEqualTo("127.0.0.1");
    }

    private Message createReplyMessage(String clusterName) {
        Message requestMessage = new Message();
        Map map = new HashMap<String, String>();
        map.put(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "127.0.0.1");
        map.put(MessageConst.PROPERTY_CLUSTER, clusterName);
        map.put(MessageConst.PROPERTY_MESSAGE_TTL, "3000");
        MessageAccessor.setProperties(requestMessage, map);
        return requestMessage;
    }

}
