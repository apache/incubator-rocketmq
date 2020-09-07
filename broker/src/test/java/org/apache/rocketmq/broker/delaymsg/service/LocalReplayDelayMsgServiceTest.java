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
package org.apache.rocketmq.broker.delaymsg.service;

import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DelayMsgCheckPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Observable;

import static org.mockito.Mockito.*;

public class LocalReplayDelayMsgServiceTest {
    private DefaultMessageStore messageStore;
    private DelayMsgCheckPoint checkPoint;
    private ScheduleIndex index;
    private LocalReplayDelayMsgService replayDelayMsgService;

    @Before
    public void setup() {
        messageStore = mock(DefaultMessageStore.class);
        checkPoint = mock(DelayMsgCheckPoint.class);
        index = mock(ScheduleIndex.class);
    }

    @Test
    public void testProcess() {
        replayDelayMsgService = new LocalReplayDelayMsgService(messageStore, checkPoint);
        long scheduleTime = System.currentTimeMillis();
        when(index.getScheduleTime()).thenReturn(scheduleTime);
        replayDelayMsgService.process(index);
        verify(checkPoint, times(1)).update(isA(Observable.class), anyLong());
    }

    @After
    public void teardown() {
        replayDelayMsgService.shutdown();
    }
}
