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
package org.apache.rocketmq.broker.delaymsg.timewheel;

import org.apache.rocketmq.broker.delaymsg.service.LocalReplayDelayMsgService;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.store.delaymsg.AbstractReplayDelayMsgService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class HashedWheelTimerTest {
    private AbstractReplayDelayMsgService replayDelayMsgService;

    @Before
    public void setup() {
        replayDelayMsgService = Mockito.mock(LocalReplayDelayMsgService.class);
        doNothing().when(replayDelayMsgService).process(isA(ScheduleIndex.class));
    }


    @Test
    public void testScheduleTimeoutShouldNotRunBeforeDelay() throws InterruptedException {
        final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryImpl("test"), 1, TimeUnit.SECONDS, 100, replayDelayMsgService);
        ScheduleIndex index = new ScheduleIndex(Arrays.asList(0L), System.currentTimeMillis());
        timer.newTimeout(index, 3, TimeUnit.SECONDS);
        Thread.sleep(1000);
        verify(replayDelayMsgService, Mockito.never()).process(isA(ScheduleIndex.class));
        timer.stop();
    }

    @Test
    public void testScheduleTimeoutShouldRunAfterDelay() throws InterruptedException {
        final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryImpl("test"), 1, TimeUnit.SECONDS, 100, replayDelayMsgService);
        ScheduleIndex index = new ScheduleIndex(Arrays.asList(0L), System.currentTimeMillis());
        timer.newTimeout(index, 1, TimeUnit.SECONDS);
        Thread.sleep(3000);
        verify(replayDelayMsgService, Mockito.times(1)).process(isA(ScheduleIndex.class));
        timer.stop();
    }

    @Test(timeout = 4000)
    public void testStopTimer() throws InterruptedException {
        final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryImpl("test"), 1, TimeUnit.SECONDS, 100, replayDelayMsgService);
        ScheduleIndex index = new ScheduleIndex(Arrays.asList(0L), System.currentTimeMillis());
        timer.newTimeout(index, 1, TimeUnit.SECONDS);
        timer.stop();
        Thread.sleep(3000);
        verify(replayDelayMsgService, Mockito.never()).process(isA(ScheduleIndex.class));
    }

    @Test(timeout = 3000)
    public void testTimerShouldThrowExceptionAfterShutdownForNewTimeouts() {
        final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryImpl("test"), 1, TimeUnit.SECONDS, 100, replayDelayMsgService);
        ScheduleIndex index = new ScheduleIndex(Arrays.asList(0L), System.currentTimeMillis());
        timer.stop();
        try {
            timer.newTimeout(index, 1, TimeUnit.SECONDS);
            fail("Expected exception didn't occur.");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testExecutionOnTime() throws InterruptedException {
        final HashedWheelTimer timer = new HashedWheelTimer(new ThreadFactoryImpl("test"), 1, TimeUnit.SECONDS, 100, replayDelayMsgService);
        ScheduleIndex index = new ScheduleIndex(Arrays.asList(0L), System.currentTimeMillis());
        timer.newTimeout(index, 1, TimeUnit.SECONDS);
        timer.newTimeout(index, 2, TimeUnit.SECONDS);
        timer.newTimeout(index, 3, TimeUnit.SECONDS);
        Thread.sleep(4000);
        verify(replayDelayMsgService, Mockito.times(3)).process(isA(ScheduleIndex.class));
        timer.stop();
    }

}
