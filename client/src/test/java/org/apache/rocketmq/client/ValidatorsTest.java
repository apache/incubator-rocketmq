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
import org.apache.rocketmq.common.MixAll;
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
}
