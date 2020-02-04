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

package org.apache.rocketmq.common.stats;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

public class StatsItemSet {
    private final ConcurrentMap<String/* key */, StatsItem> statsItemTable =
        new ConcurrentHashMap<String, StatsItem>(128);

    private final String statsName;
    private final ScheduledExecutorService scheduledExecutorService;
    private final InternalLogger log;

    public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    private void samplingInSeconds() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInSeconds();
        }
    }

    private void samplingInMinutes() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInMinutes();
        }
    }

    private void samplingInHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().samplingInHour();
        }
    }

    private void printAtMinutes() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtMinutes();
        }
    }

    private void printAtHour() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtHour();
        }
    }

    private void printAtDay() {
        Iterator<Entry<String, StatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, StatsItem> next = it.next();
            next.getValue().printAtDay();
        }
    }

    public void addValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().addAndGet(incValue);
        statsItem.getTimes().addAndGet(incTimes);
    }

    public StatsItem getAndCreateStatsItem(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            StatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }

    public StatsSnapshot getStatsDataInMinute(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInMinute();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInHour(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInHour();
        }
        return new StatsSnapshot();
    }

    public StatsSnapshot getStatsDataInDay(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInDay();
        }
        return new StatsSnapshot();
    }

    public StatsItem getStatsItem(final String statsKey) {
        return this.statsItemTable.get(statsKey);
    }
}
