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

package org.apache.rocketmq.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;

public class ValidatorsTest {

    @Test
    public void testCheckTopic_Success() throws MQClientException {
        Validators.checkTopic("Hello");
        Validators.checkTopic("%RETRY%Hello");
        Validators.checkTopic("_%RETRY%Hello");
        Validators.checkTopic("-%RETRY%Hello");
        Validators.checkTopic("223-%RETRY%Hello");
    }

    @Test
    public void testCheckTopic_HasIllegalCharacters() {
        String illegalTopic = "TOPIC&*^";
        try {
            Validators.checkTopic(illegalTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("The specified topic[%s] contains illegal characters, allowing only %s", illegalTopic, Validators.VALID_PATTERN_STR));
        }
    }

    @Test
    public void testCheckTopic_UseDefaultTopic() {
        String defaultTopic = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
        try {
            Validators.checkTopic(defaultTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("The topic[%s] is conflict with AUTO_CREATE_TOPIC_KEY_TOPIC.", defaultTopic));
        }
    }

    @Test
    public void testCheckTopic_BlankTopic() {
        String blankTopic = "";
        try {
            Validators.checkTopic(blankTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("The specified topic is blank");
        }
    }

    @Test
    public void testCheckTopic_TooLongTopic() {
        String tooLongTopic = StringUtils.rightPad("TooLongTopic", Validators.TOPIC_MAX_LENGTH + 1, "_");
        assertThat(tooLongTopic.length()).isGreaterThan(Validators.TOPIC_MAX_LENGTH);
        try {
            Validators.checkTopic(tooLongTopic);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("The specified topic is longer than topic max length");
        }
    }

    @Test
    public void testCheckGroup_Success() throws MQClientException {
        Validators.checkGroup("Group");
        Validators.checkGroup("%TEST%Group");
        Validators.checkGroup("_%TEST%Group");
        Validators.checkGroup("-%TEST%Group");
        Validators.checkGroup("223-%TEST%Group");
    }

    @Test
    public void testCheckGroup_BlankGroup() {
        String blankGroup = "";
        try {
            Validators.checkGroup(blankGroup);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("the specified group is blank");
        }
    }

    @Test
    public void testCheckGroup_HasIllegalCharacters() {
        String illegalGroup = "GROUP&*^";
        try {
            Validators.checkGroup(illegalGroup);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith(String.format("the specified group[%s] contains illegal characters, allowing only %s", illegalGroup, Validators.VALID_PATTERN_STR));
        }
    }

    @Test
    public void testCheckGroup_TooLongGroup() {
        String tooLongGroup = StringUtils.rightPad("TooLongGroup", Validators.CHARACTER_MAX_LENGTH + 1, "_");
        assertThat(tooLongGroup.length()).isGreaterThan(Validators.CHARACTER_MAX_LENGTH);
        try {
            Validators.checkGroup(tooLongGroup);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageStartingWith("the specified group is longer than group max length 255.");
        }
    }

    @Test
    public void testCheckMessage_Success() throws MQClientException {
        Message message = new Message();
        message.setTopic("hello");
        message.setBody("test msg body".getBytes());

        DefaultMQProducer producer = new DefaultMQProducer();
        Validators.checkMessage(message, producer);
    }

    @Test
    public void testCheckMessage_NullMessage() {
        Message nullMessage = null;
        try {
            Validators.checkMessage(nullMessage, null);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("the message is null");
        }
    }

    @Test
    public void testCheckMessage_NullMessageBody() {
        Message messageWithNullBody = new Message();
        messageWithNullBody.setTopic("hello");
        messageWithNullBody.setBody(null);
        try {
            Validators.checkMessage(messageWithNullBody, null);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("the message body is null");
        }
    }

    @Test
    public void testCheckMessage_ZeroMessageBodyLen() {
        Message messageWithZeroBodyLen = new Message();
        messageWithZeroBodyLen.setTopic("hello");
        messageWithZeroBodyLen.setBody(new byte[0]);
        try {
            Validators.checkMessage(messageWithZeroBodyLen, null);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("the message body length is zero");
        }
    }

    @Test
    public void testCheckMessage_TooLongMessageBodyLen() {
        DefaultMQProducer producer = new DefaultMQProducer();

        Message messageWithTooLongBodyLen = new Message();
        messageWithTooLongBodyLen.setTopic("hello");
        messageWithTooLongBodyLen.setBody(new byte[producer.getMaxMessageSize() + 1]);

        assertThat(messageWithTooLongBodyLen.getBody().length).isGreaterThan(producer.getMaxMessageSize());

        try {
            Validators.checkMessage(messageWithTooLongBodyLen, producer);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining(String.format("the message body size over max value, MAX: %d", producer.getMaxMessageSize()));
        }
    }

    @Test
    public void testRegularExpressionMatcher_NullPattern() {
        assertThat(Validators.regularExpressionMatcher("testOrigin", null)).isTrue();
    }
}
