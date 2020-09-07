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
import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DelayMsgCheckPoint;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.delaymsg.AbstractReplayDelayMsgService;
import org.apache.rocketmq.store.index.DelayMsgIndexService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Observable;

import static org.mockito.Mockito.*;

public class TimeWheelManagerTest {

    private AbstractReplayDelayMsgService replayDelayMsgService;
    private DelayMsgIndexService delayMsgIndexService;
    private DelayMsgCheckPoint delayMsgCheckPoint;
    TimeWheelManager timeWheelManager;

    @Before
    public void setup() {
        replayDelayMsgService = Mockito.mock(LocalReplayDelayMsgService.class);
        delayMsgIndexService = Mockito.mock(DelayMsgIndexService.class);
        delayMsgCheckPoint = Mockito.mock(DelayMsgCheckPoint.class);
        timeWheelManager = spy(new TimeWheelManager(replayDelayMsgService, delayMsgIndexService, delayMsgCheckPoint));
    }

    @Test
    public void testInit() {

        verify(delayMsgIndexService, times(1)).addObserver(isA(TimeWheelManager.class));
    }

    @Test
    public void testStart() {
        timeWheelManager.start();
        verify(replayDelayMsgService, times(1)).start();
    }

    @Test
    public void testUpdate() throws NoSuchFieldException, IllegalAccessException {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        MessageStoreConfig storeConfig = mock(MessageStoreConfig.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(storeConfig);
        when(storeConfig.getMaxHashSlotNum()).thenReturn(1000);
        when(storeConfig.getMaxIndexNum()).thenReturn(1000);
        when(storeConfig.getStorePathRootDir()).thenReturn("path");
        delayMsgIndexService = new DelayMsgIndexService(messageStore);
        timeWheelManager.shutdown();
        timeWheelManager = spy(new TimeWheelManager(replayDelayMsgService, delayMsgIndexService, delayMsgCheckPoint));
        delayMsgIndexService.addObserver(timeWheelManager);
        Field field = DelayMsgIndexService.class.getSuperclass().getDeclaredField("changed");
        field.setAccessible(true);
        field.set(delayMsgIndexService, true);
        delayMsgIndexService.notifyObservers(mock(ScheduleIndex.class));
        verify(timeWheelManager, times(1)).update(isA(Observable.class), isA(Object.class));
    }

    @Test
    public void testShutdown() {
        timeWheelManager.shutdown();
        verify(replayDelayMsgService, times(1)).shutdown();
    }

    @After
    public void teardown() {
        timeWheelManager.shutdown();
    }


}
