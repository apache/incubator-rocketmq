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
package org.apache.rocketmq.broker.latency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public class BrokerFastFailure {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "BrokerFastFailureScheduledThread"));
    private final BrokerController brokerController;

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt object = (FutureTaskExt) runnable;
                return (RequestTask) object.getRunnable();
            }
        } catch (Throwable e) {
            log.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()) {
                    cleanExpiredRequest();
                }
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void cleanExpiredRequest() {
        while (this.brokerController.getMessageStore().isOSPageCacheBusy()) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty()) {
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }

                    final RequestTask rt = castRunnable(runnable);
                    rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", System.currentTimeMillis() - rt.getCreateTimestamp(), this.brokerController.getSendThreadPoolQueue().size()));
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }

        cleanExpiredRequestInQueue(this.brokerController.getSendThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue());

        cleanExpiredRequestInQueue(this.brokerController.getPullThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInPullQueue());

        cleanExpiredRequestInQueue(this.brokerController.getHeartbeatThreadPoolQueue(),
            this.brokerController.getBrokerConfig().getWaitTimeMillsInHeartbeatQueue());

        cleanExpiredRequestInQueue(this.brokerController.getEndTransactionThreadPoolQueue(), this
            .brokerController.getBrokerConfig().getWaitTimeMillsInTransactionQueue());
    }

    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        while (true) {
            try {
                if (!blockingQueue.isEmpty()) {
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format("[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d", behind, blockingQueue.size()));
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
