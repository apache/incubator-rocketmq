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

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckClientRequestBodyTest {

    @Test
    public void testFromJson() {
        SubscriptionData subscriptionData = new SubscriptionData();
        String expectedClientId = "defalutId";
        String expectedGroup = "defaultGroup";
        CheckClientRequestBody checkClientRequestBody = new CheckClientRequestBody();
        checkClientRequestBody.setClientId(expectedClientId);
        checkClientRequestBody.setGroup(expectedGroup);
        checkClientRequestBody.setSubscriptionData(subscriptionData);
        String json = RemotingSerializable.toJson(checkClientRequestBody, true);
        CheckClientRequestBody fromJson = RemotingSerializable.fromJson(json, CheckClientRequestBody.class);
        assertThat(fromJson.getClientId()).isEqualTo(expectedClientId);
        assertThat(fromJson.getGroup()).isEqualTo(expectedGroup);
        assertThat(fromJson.getSubscriptionData()).isEqualTo(subscriptionData);
    }
}