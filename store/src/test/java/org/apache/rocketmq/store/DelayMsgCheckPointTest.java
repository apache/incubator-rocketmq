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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.assertj.core.api.Assertions.assertThat;

public class DelayMsgCheckPointTest {
    @Test
    public void testWriteAndRead() throws IOException, ParseException {
        DelayMsgCheckPoint delayMsgCheckPoint = new DelayMsgCheckPoint("target/checkpoint_test/0000");
        long startDeliverTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-08-14 09:47:00").getTime();
        ;
        delayMsgCheckPoint.setDelayMsgTimestamp(startDeliverTime);
        delayMsgCheckPoint.flush();

        long result = delayMsgCheckPoint.getDelayMsgTimestamp();
        assertThat(result).isEqualTo(startDeliverTime);
        delayMsgCheckPoint.shutdown();
        delayMsgCheckPoint = new DelayMsgCheckPoint("target/checkpoint_test/0000");
        assertThat(delayMsgCheckPoint.getDelayMsgTimestamp()).isEqualTo(startDeliverTime);
    }

    @After
    public void destory() {
        File file = new File("target/checkpoint_test");
        UtilAll.deleteFile(file);
    }
}
