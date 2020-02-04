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
package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicValidatorTest {

    @Test
    public void testTopicValidator_NotPass() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        Boolean res = TopicValidator.validateTopic("", response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic is blank");

        clearResponse(response);
        res = TopicValidator.validateTopic("../TopicTest", response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic contains illegal characters");

        clearResponse(response);
        res = TopicValidator.validateTopic(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC, response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic is conflict with AUTO_CREATE_TOPIC_KEY_TOPIC.");

        clearResponse(response);
        res = TopicValidator.validateTopic(generateString(128), response);
        assertThat(res).isFalse();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("The specified topic is longer than topic max length.");

    }

    @Test
    public void testTopicValidator_Pass() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        Boolean res = TopicValidator.validateTopic("TestTopic", response);
        assertThat(res).isTrue();
        assertThat(response.getCode()).isEqualTo(-1);
        assertThat(response.getRemark()).isEmpty();
    }

    private static void clearResponse(RemotingCommand response) {
        response.setCode(-1);
        response.setRemark("");
    }

    private static String generateString(int length) {
        StringBuilder stringBuffer = new StringBuilder();
        String tmpStr = "0123456789";
        for (int i = 0; i < length; i++) {
            stringBuffer.append(tmpStr);
        }
        return stringBuffer.toString();
    }
}
