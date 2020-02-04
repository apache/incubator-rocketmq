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

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;
import java.util.concurrent.ConcurrentHashMap;
import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionGroupWrapperTest {

    @Test
    public void testFromJson(){
        SubscriptionGroupWrapper subscriptionGroupWrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptions = new ConcurrentHashMap<String, SubscriptionGroupConfig>();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setBrokerId(1234);
        subscriptionGroupConfig.setGroupName("Consumer-group-one");
        subscriptions.put("Consumer-group-one", subscriptionGroupConfig);
        subscriptionGroupWrapper.setSubscriptionGroupTable(subscriptions);
        DataVersion dataVersion = new DataVersion();
        dataVersion.nextVersion();
        subscriptionGroupWrapper.setDataVersion(dataVersion);
        String json = RemotingSerializable.toJson(subscriptionGroupWrapper, true);
        SubscriptionGroupWrapper fromJson = RemotingSerializable.fromJson(json, SubscriptionGroupWrapper.class);
        assertThat(fromJson.getSubscriptionGroupTable()).containsKey("Consumer-group-one");
        assertThat(fromJson.getSubscriptionGroupTable().get("Consumer-group-one").getGroupName()).isEqualTo("Consumer-group-one");
        assertThat(fromJson.getSubscriptionGroupTable().get("Consumer-group-one").getBrokerId()).isEqualTo(1234);
    }

}
