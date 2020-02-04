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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class TopicValidator {

    private static final String VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$";
    private static final Pattern PATTERN = Pattern.compile(VALID_PATTERN_STR);
    private static final int TOPIC_MAX_LENGTH = 127;

    private static boolean regularExpressionMatcher(String origin, Pattern pattern) {
        if (pattern == null) {
            return true;
        }
        Matcher matcher = pattern.matcher(origin);
        return matcher.matches();
    }

    public static boolean validateTopic(String topic, RemotingCommand response) {

        if (UtilAll.isBlank(topic)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic is blank.");
            return false;
        }

        if (!regularExpressionMatcher(topic, PATTERN)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic contains illegal characters, allowing only " + VALID_PATTERN_STR);
            return false;
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic is longer than topic max length.");
            return false;
        }

        //whether the same with system reserved keyword
        if (topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic is conflict with AUTO_CREATE_TOPIC_KEY_TOPIC.");
            return false;
        }

        return true;
    }
}
