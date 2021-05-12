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

package org.apache.rocketmq.remoting.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadFactoryImpl implements ThreadFactory {
    private final AtomicLong threadIndex = new AtomicLong(0);
    private final String threadNamePrefix;
    private final int threads;
    private final boolean daemon;

    public ThreadFactoryImpl(final String threadNamePrefix) {
        this(threadNamePrefix, 0, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, final int threads) {
        this(threadNamePrefix, threads, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, final int threads, final boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.threads = threads;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = threads > 0 ? String.format("%s_%d_%d", threadNamePrefix, threads, this.threadIndex.incrementAndGet())
                : String.format("%s_%d", threadNamePrefix, this.threadIndex.incrementAndGet());
        Thread thread = new Thread(r, name);
        thread.setDaemon(daemon);
        return thread;
    }
}
