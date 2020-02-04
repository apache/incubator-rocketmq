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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MixAllTest {
    @Test
    public void testGetLocalInetAddress() throws Exception {
        List<String> localInetAddress = MixAll.getLocalInetAddress();
        String local = InetAddress.getLocalHost().getHostAddress();
        assertThat(localInetAddress).contains("127.0.0.1");
        assertThat(local).isNotNull();
    }

    @Test
    public void testBrokerVIPChannel() {
        assertThat(MixAll.brokerVIPChannel(true, "127.0.0.1:10911")).isEqualTo("127.0.0.1:10909");
    }

    @Test
    public void testCompareAndIncreaseOnly() {
        AtomicLong target = new AtomicLong(5);
        assertThat(MixAll.compareAndIncreaseOnly(target, 6)).isTrue();
        assertThat(target.get()).isEqualTo(6);

        assertThat(MixAll.compareAndIncreaseOnly(target, 4)).isFalse();
        assertThat(target.get()).isEqualTo(6);
    }

    @Test
    public void testFile2String() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "MixAllTest" + System.currentTimeMillis();
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        PrintWriter out = new PrintWriter(fileName);
        out.write("TestForMixAll");
        out.close();
        String string = MixAll.file2String(fileName);
        assertThat(string).isEqualTo("TestForMixAll");
        file.delete();
    }

    @Test
    public void testFile2String_WithChinese() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "MixAllTest" + System.currentTimeMillis();
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        PrintWriter out = new PrintWriter(fileName);
        out.write("TestForMixAll_中文");
        out.close();
        String string = MixAll.file2String(fileName);
        assertThat(string).isEqualTo("TestForMixAll_中文");
        file.delete();
    }

    @Test
    public void testString2File() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "MixAllTest" + System.currentTimeMillis();
        MixAll.string2File("MixAll_testString2File", fileName);
        assertThat(MixAll.file2String(fileName)).isEqualTo("MixAll_testString2File");
    }

    @Test
    public void testGetLocalhostByNetworkInterface() throws Exception {
        assertThat(MixAll.LOCALHOST).isNotNull();
        assertThat(MixAll.getLocalhostByNetworkInterface()).isNotNull();
    }
}
