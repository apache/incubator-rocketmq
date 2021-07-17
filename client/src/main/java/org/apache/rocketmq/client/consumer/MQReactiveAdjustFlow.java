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
package org.apache.rocketmq.client.consumer;
import java.security.Key;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;

/**
 *flow controller be measured in consumer,just like the sliding window of tcp
 */
public class MQReactiveAdjustFlow {
    /**
     * the max and the min time to check
     */
    public static final int MAX_CHECK_TIME = 60000;
    public static final int MIN_CHECK_SAMPLE = 10;
    private volatile long lastTimeCheck = System.currentTimeMillis();

    private static final InternalLogger log = ClientLogger.getLog();

    /**
     * local cache
     */
    private static final ConcurrentHashMap<String/*GroupName*/, MQReactiveAdjustFlow> HOLDER_CACHE = new ConcurrentHashMap<>();

    /**
     * store the num of message in this queue in broker
     */
    public ConcurrentHashMap<Integer, Long> allQueueAccumulation = new ConcurrentHashMap<>();

    /**
     * the interval time to pull the message from broker
     */
    private volatile long currentPullInterval = 0;

    /**
     * The maximum of interval time allowed by the system
     */
    public static final int MAX_PULL_INTERVAL_TIME = 60000;

    private AtomicBoolean isProcess = new AtomicBoolean(false);


    public MQReactiveAdjustFlow(long concurrentPullInterval) {
        this.currentPullInterval = concurrentPullInterval;
    }

    /**
     *
     * @param consumer comsumer
     * @param queueId the id of queue
     * @param offset the offset of the first message in the list
     * @param queueMaxOffSet the max offset of this queue
     * @param fullSize the maximum amount of the stacked message that can be tolerated
     * @param minPullInterval the min interval time
     * @param maxPullInterval the max interval time
     * @param defaultCheckTime the time that you want to check the Consumption rate
     * @param defaultStep the step
     */
    public static void adjustFlow(
        DefaultMQPushConsumer consumer,
        int queueId,
        long offset,
        long queueMaxOffSet,
        long fullSize,
        int minPullInterval,
        int maxPullInterval,
        long defaultCheckTime,
        long defaultStep,
        long initInterval) {
        //check the params
        if (minPullInterval > MAX_PULL_INTERVAL_TIME) {
            maxPullInterval = 1000;
        }
        if (maxPullInterval > MAX_PULL_INTERVAL_TIME) {
            maxPullInterval = MAX_PULL_INTERVAL_TIME;
        }
        if (minPullInterval >= maxPullInterval) {
            minPullInterval = maxPullInterval / 2;
        }

        if (defaultCheckTime < MIN_CHECK_SAMPLE || defaultCheckTime > MAX_CHECK_TIME) {
            defaultCheckTime = 1000;
        }

        MQReactiveAdjustFlow flow = HOLDER_CACHE.computeIfAbsent(consumer.getInstanceName(),
            key -> new MQReactiveAdjustFlow(initInterval));

        flow.allQueueAccumulation.put(queueId, queueMaxOffSet - offset);
        if (System.currentTimeMillis() - flow.lastTimeCheck > defaultCheckTime) {
//            System.out.printf("the queueId is: %s, the num of the message that not be comsumed: %s, the currentPullInterval: %s \n",
//                queueId,queueMaxOffSet-offset,flow.currentPullInterval);

            if (flow.isProcess.compareAndSet(false, true)) {
                try {
                    long lastPullInterval = consumer.getPullInterval();
                    long maxQueueOffset = 0;
                    for (Long value : flow.allQueueAccumulation.values()) {
                        if (value > maxQueueOffset) {
                            maxQueueOffset = value;
                        }
                    }
                    //change the PullInterval
                    if (maxQueueOffset > fullSize) {
                        if (flow.currentPullInterval - defaultStep <= minPullInterval) {
                            flow.currentPullInterval = minPullInterval;
                        } else {
                            flow.currentPullInterval = flow.currentPullInterval - defaultStep;
                        }
                    } else {
                        if (flow.currentPullInterval + defaultStep >= maxPullInterval) {
                            flow.currentPullInterval = maxPullInterval;
                        } else {
                            flow.currentPullInterval = flow.currentPullInterval + defaultStep;
                        }
                    }
                    if (lastPullInterval != flow.currentPullInterval) {
                        synchronized (consumer) {
                            consumer.setPullInterval(flow.currentPullInterval);
                        }
                        log.warn("adjusting MQ consumer-thread:{}, group:{}, queueId:{}, lastPullInterval:{}, " +
                                "currentPullInterval:{}, " + "defaultStep:{}",
                            Thread.currentThread().getName(), consumer.getConsumerGroup(), queueId,
                            lastPullInterval, flow.currentPullInterval, defaultStep);
                    }
                } catch (Exception e) {
                    log.error("adjusting MQ consumer-thread:{}, group:{}, queueId:{}, exception:{}",
                        Thread.currentThread().getName(), consumer.getConsumerGroup(), queueId, e);
                } finally {
                    flow.lastTimeCheck = System.currentTimeMillis();
                    flow.isProcess.set(false);
                }
            }
        }
    }

}
