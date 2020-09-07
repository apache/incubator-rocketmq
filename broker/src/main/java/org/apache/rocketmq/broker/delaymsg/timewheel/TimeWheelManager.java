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

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DelayMsgCheckPoint;
import org.apache.rocketmq.store.delaymsg.AbstractReplayDelayMsgService;
import org.apache.rocketmq.store.index.DelayMsgIndexService;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimeWheelManager implements Observer {
    public static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);

    private static final int LOAD_DURATION = 3600;
    private HashedWheelTimer timer;
    private AbstractReplayDelayMsgService replayDelayMsgService;
    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TimerLoadOrShiftThread"));

    private DelayMsgIndexService delayMsgIndexService;
    private DelayMsgCheckPoint delayMsgCheckPoint;

    private volatile long preLoadBeginTime;

    public TimeWheelManager(AbstractReplayDelayMsgService replayDelayMsgService, DelayMsgIndexService delayMsgIndexService, DelayMsgCheckPoint delayMsgCheckPoint) {
        this.replayDelayMsgService = replayDelayMsgService;
        this.delayMsgIndexService = delayMsgIndexService;
        this.delayMsgCheckPoint = delayMsgCheckPoint;
        init();
    }

    public void init() {
        long begin = System.currentTimeMillis();
        this.timer = new HashedWheelTimer(new ThreadFactoryImpl("HashedWheelTimer_"), 1, TimeUnit.SECONDS, 2 * LOAD_DURATION, replayDelayMsgService);
        long end = System.currentTimeMillis();
        LOGGER.info("HashedTimeWheel has been created, elapsedTime is {} milliSeconds. TickDuration: {}, Unit: {}, TicksPerWheel: {}", end - begin, 1, "Second", 2 * 60 * 60);
        this.delayMsgIndexService.addObserver(this);
    }

    public void start() {
        initLoad();
        this.replayDelayMsgService.start();
        scheduledExecutorService.scheduleAtFixedRate(this::preLoad, 30, 60, TimeUnit.MINUTES);
    }

    public void shutdown() {
        this.timer.stop();
        this.scheduledExecutorService.shutdown();
        this.replayDelayMsgService.shutdown();
    }

    private void preLoad() {
        preLoadBeginTime += LOAD_DURATION * 1000;
        LOGGER.info("HashedWheelTimer preLoad...");
        if (!this.delayMsgIndexService.isFileExist()) {
            LOGGER.info("There is no delayMsg during the time window [{}, {}), preload nothing.", UtilAll.timeMillisToHumanString2(preLoadBeginTime), UtilAll.timeMillisToHumanString2(preLoadBeginTime + LOAD_DURATION * 1000));
            return;
        }
        long begin = System.currentTimeMillis();
        int delayMsgCount = 0;

        long preLoadEndTime = preLoadBeginTime + LOAD_DURATION * 1000;

        for (long startDeliverTime = preLoadBeginTime; startDeliverTime < preLoadEndTime; startDeliverTime += 1000) {
            delayMsgCount += this.load(startDeliverTime);
        }
        long end = System.currentTimeMillis();
        LOGGER.info("Preload delayMsg end, {} delayMsg(s) is loaded, startDeliverTime range is [{}, {}),  elapsed time is: {} milliSeconds", delayMsgCount, UtilAll.timeMillisToHumanString2(preLoadBeginTime), UtilAll.timeMillisToHumanString2(preLoadEndTime), end - begin);

    }

    private void initLoad() {
        LOGGER.info("Starting HashedWheelTimer initLoad...");
        preLoadBeginTime = this.getCurrentTime();
        if (!this.delayMsgIndexService.isFileExist()) {
            LOGGER.info("There is no delayMsg, load nothing.");
            return;
        }
        long begin = System.currentTimeMillis();
        int delayMsgCount = 0;
        long loadBeginTime = this.delayMsgCheckPoint.getDelayMsgTimestamp();
        if (loadBeginTime == 0) {
            loadBeginTime = preLoadBeginTime;
        }
        long loadEndTime = preLoadBeginTime + LOAD_DURATION * 1000;

        for (long startDeliverTime = loadBeginTime; startDeliverTime < loadEndTime; startDeliverTime += 1000) {
            delayMsgCount += this.load(startDeliverTime);
        }
        long end = System.currentTimeMillis();
        LOGGER.info("HashedWheelTimer initLoad end, {} delayMsg(s) is loaded, the delayMsgs startDeliverTime range is: [{}, {}), elapsedTime is {} milliSeconds",
                delayMsgCount, UtilAll.timeMillisToHumanString2(loadBeginTime), UtilAll.timeMillisToHumanString2(loadEndTime), end - begin);
    }

    private int load(long startDeliverTime) {
        ScheduleIndex scheduleIndex = this.delayMsgIndexService.queryDelayMsgOffset(startDeliverTime);
        int delayMsgCount = scheduleIndex.getPhyOffsets().size();
        if (scheduleIndex.getPhyOffsets().size() > 0) {
            timer.newTimeout(scheduleIndex, (startDeliverTime - this.getCurrentTime()) / 1000, TimeUnit.SECONDS);
        }
        return delayMsgCount;

    }


    private void updateTimer(ScheduleIndex scheduleIndex) {
        long startDeliverTime = scheduleIndex.getScheduleTime();
        if (startDeliverTime - preLoadBeginTime < LOAD_DURATION * 1000) {
            LOGGER.debug("Update HashedWheelTimer, added delayMsg startDeliverTime is {}.", UtilAll.timeMillisToHumanString2(startDeliverTime));
            timer.newTimeout(scheduleIndex, (startDeliverTime - getCurrentTime()) / 1000, TimeUnit.SECONDS);
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        updateTimer((ScheduleIndex) arg);
    }


    private long getCurrentTime() {
        return System.currentTimeMillis() / 1000 * 1000;
    }
}
