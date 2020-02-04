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

package org.apache.rocketmq.common.protocol.heartbeat;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.assertj.core.util.Sets;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionDataTest {

    @Test
    public void testConstructor1() {
        SubscriptionData subscriptionData = new SubscriptionData();
        assertThat(subscriptionData.getTopic()).isNull();
        assertThat(subscriptionData.getSubString()).isNull();
        assertThat(subscriptionData.getSubVersion()).isLessThanOrEqualTo(System.currentTimeMillis());
        assertThat(subscriptionData.getExpressionType()).isEqualTo(ExpressionType.TAG);
        assertThat(subscriptionData.getFilterClassSource()).isNull();
        assertThat(subscriptionData.getCodeSet()).isEmpty();
        assertThat(subscriptionData.getTagsSet()).isEmpty();
        assertThat(subscriptionData.isClassFilterMode()).isFalse();
    }

    @Test
    public void testConstructor2() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        assertThat(subscriptionData.getTopic()).isEqualTo("TOPICA");
        assertThat(subscriptionData.getSubString()).isEqualTo("*");
        assertThat(subscriptionData.getSubVersion()).isLessThanOrEqualTo(System.currentTimeMillis());
        assertThat(subscriptionData.getExpressionType()).isEqualTo(ExpressionType.TAG);
        assertThat(subscriptionData.getFilterClassSource()).isNull();
        assertThat(subscriptionData.getCodeSet()).isEmpty();
        assertThat(subscriptionData.getTagsSet()).isEmpty();
        assertThat(subscriptionData.isClassFilterMode()).isFalse();
    }


    @Test
    public void testHashCodeNotEquals() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        subscriptionData.setCodeSet(Sets.newLinkedHashSet(1, 2, 3));
        subscriptionData.setTagsSet(Sets.newLinkedHashSet("TAGA", "TAGB", "TAG3"));
        assertThat(subscriptionData.hashCode()).isNotEqualTo(System.identityHashCode(subscriptionData));
    }

    @Test
    public void testFromJson() throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        subscriptionData.setFilterClassSource("TestFilterClassSource");
        subscriptionData.setCodeSet(Sets.newLinkedHashSet(1, 2, 3));
        subscriptionData.setTagsSet(Sets.newLinkedHashSet("TAGA", "TAGB", "TAG3"));
        String json = RemotingSerializable.toJson(subscriptionData, true);
        SubscriptionData fromJson = RemotingSerializable.fromJson(json, SubscriptionData.class);
        assertThat(subscriptionData).isEqualTo(fromJson);
        assertThat(subscriptionData).isEqualByComparingTo(fromJson);
        assertThat(subscriptionData.getFilterClassSource()).isEqualTo("TestFilterClassSource");
        assertThat(fromJson.getFilterClassSource()).isNull();
    }


    @Test
    public void testCompareTo() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        SubscriptionData subscriptionData1 = new SubscriptionData("TOPICBA", "*");
        assertThat(subscriptionData.compareTo(subscriptionData1)).isEqualTo("TOPICA@*".compareTo("TOPICB@*"));
    }
}
