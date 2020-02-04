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

package org.apache.rocketmq.common.message;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageClientIDSetterTest {

    @Test
    public void testGetIPStrFromID() {
        String ipv4HostMsgId = "C0A803CA00002A9F0000000000031367";
        String ipv6HostMsgId = "24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0";
        String v4Ip = "192.168.3.202";
        String v6Ip = "2408:4004:0180:8100:3faa:1dde:2b3f:898a";
        assertThat(MessageClientIDSetter.getIPStrFromID(ipv4HostMsgId)).isEqualTo(v4Ip);
        assertThat(MessageClientIDSetter.getIPStrFromID(ipv6HostMsgId)).isEqualTo(v6Ip);
    }

}
