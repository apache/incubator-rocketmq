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
package org.apache.rocketmq.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerConfigTest {

    @Test
    public void testConsumerFallBehindThresholdOverflow() {
        long expect = 1024L * 1024 * 1024 * 16;
        assertThat(new BrokerConfig().getConsumerFallbehindThreshold()).isEqualTo(expect);
    }

    @Test
    public void testBrokerConfigAttribute() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        brokerConfig.setAutoCreateTopicEnable(false);
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setBrokerId(0);
        brokerConfig.setBrokerClusterName("DefaultCluster");
        brokerConfig.setMsgTraceTopicName("RMQ_SYS_TRACE_TOPIC4");
        assertThat(brokerConfig.getBrokerClusterName()).isEqualTo("DefaultCluster");
        assertThat(brokerConfig.getNamesrvAddr()).isEqualTo("127.0.0.1:9876");
        assertThat(brokerConfig.getMsgTraceTopicName()).isEqualTo("RMQ_SYS_TRACE_TOPIC4");
        assertThat(brokerConfig.getBrokerId()).isEqualTo(0);
        assertThat(brokerConfig.getBrokerName()).isEqualTo("broker-a");
        assertThat(brokerConfig.isAutoCreateTopicEnable()).isEqualTo(false);
    }
}