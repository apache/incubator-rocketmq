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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ProcessQueueTest {

    @Test
    public void testCachedMessageCount() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        assertThat(pq.getMsgCount().get()).isEqualTo(100);

        pq.takeMessages(10);
        pq.commit();

        assertThat(pq.getMsgCount().get()).isEqualTo(90);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        assertThat(pq.getMsgCount().get()).isEqualTo(89);
    }

    @Test
    public void testMakeMessageToConsumeAgain() {
        ProcessQueue pq = new ProcessQueue();

        MessageExt messageExt1 = new MessageExt();
        messageExt1.setQueueOffset(1L);

        MessageExt messageExt2 = new MessageExt();
        messageExt1.setQueueOffset(2L);

        pq.makeMessageToConsumeAgain(Arrays.asList(messageExt1));
        pq.makeMessageToCosumeAgain(Arrays.asList(messageExt2));

        assertThat(pq.getMsgTreeMap().size()).isEqualTo(2);
    }

    @Test
    public void testCachedMessageSize() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        assertThat(pq.getMsgSize().get()).isEqualTo(100 * 123);

        pq.takeMessages(10);
        pq.commit();

        assertThat(pq.getMsgSize().get()).isEqualTo(90 * 123);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        assertThat(pq.getMsgSize().get()).isEqualTo(89 * 123);
    }

    @Test
    public void testFillProcessQueueInfo() {
        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(createMessageList(102400));

        ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
        pq.fillProcessQueueInfo(processQueueInfo);

        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(12);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(10);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(9);

        pq.takeMessages(80000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(0);
    }

    private List<MessageExt> createMessageList() {
        return createMessageList(100);
    }

    private List<MessageExt> createMessageList(int count) {
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < count; i++) {
            MessageExt messageExt = new MessageExt();
            messageExt.setQueueOffset(i);
            messageExt.setBody(new byte[123]);
            messageExtList.add(messageExt);
        }
        return messageExtList;
    }
}