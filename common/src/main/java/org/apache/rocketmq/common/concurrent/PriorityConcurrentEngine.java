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
package org.apache.rocketmq.common.concurrent;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.UtilAll;

public class PriorityConcurrentEngine extends ConcurrentEngine {

    /**
     * highest priority
     */
    public static final Integer MAX_PRIORITY = Integer.MIN_VALUE;

    /**
     * lowest priority
     */
    public static final Integer MIN_PRIORITY = Integer.MAX_VALUE;

    private final StagedConcurrentConsumeService consumeService = new StagedConcurrentConsumeService(this);

    private final ConcurrentNavigableMap<Integer, Queue<Object>> priorityTasks = new ConcurrentSkipListMap<>();

    public PriorityConcurrentEngine() {
        super();
    }

    public PriorityConcurrentEngine(ExecutorService enginePool) {
        super(enginePool);
    }

    public final void runPriorityAsync(Runnable... tasks) {
        runPriorityAsync(MIN_PRIORITY, tasks);
    }

    public final void runPriorityAsync(Queue<Runnable> tasks) {
        runPriorityAsync(MIN_PRIORITY, tasks);
    }

    public final void runPriorityAsync(Collection<Runnable> tasks) {
        runPriorityAsync(MIN_PRIORITY, tasks);
    }

    public final void runPriorityAsync(Integer priority, Runnable... tasks) {
        runPriorityAsync(priority, UtilAll.newArrayList(tasks));
    }

    public final void runPriorityAsync(Integer priority, Queue<? extends Runnable> tasks) {
        runPriorityAsync(priority, pollAllTask(tasks));
    }

    public final void runPriorityAsync(Integer priority, Collection<? extends Runnable> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        Queue<Object> queue = priorityTasks.putIfAbsent(priority, new ConcurrentLinkedQueue<>());
        if (null == queue) {
            queue = priorityTasks.get(priority);
        }
        for (Runnable runnable : tasks) {
            queue.offer(runnable);
        }
    }

    @SafeVarargs
    public final <T> void supplyPriorityAsync(CallableSupplier<T>... tasks) {
        supplyPriorityAsync(MIN_PRIORITY, tasks);
    }

    public final <T> void supplyPriorityAsync(Queue<? extends CallableSupplier<T>> tasks) {
        supplyPriorityAsync(MIN_PRIORITY, tasks);
    }

    public final <T> void supplyPriorityAsync(Collection<? extends CallableSupplier<T>> tasks) {
        supplyPriorityAsync(MIN_PRIORITY, tasks);
    }

    @SafeVarargs
    public final <T> void supplyPriorityAsync(Integer priority, CallableSupplier<T>... tasks) {
        supplyPriorityAsync(priority, UtilAll.newArrayList(tasks));
    }

    public final <T> void supplyPriorityAsync(Integer priority, Queue<? extends CallableSupplier<T>> tasks) {
        supplyPriorityAsync(priority, pollAllTask(tasks));
    }

    public final <T> void supplyPriorityAsync(Integer priority, Collection<? extends CallableSupplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        Queue<Object> queue = priorityTasks.putIfAbsent(priority, new ConcurrentLinkedQueue<>());
        if (null == queue) {
            queue = priorityTasks.get(priority);
        }
        for (CallableSupplier<T> supplier : tasks) {
            queue.offer(supplier);
        }
    }

    public void invokeAllNow() {
        if (super.isShutdown()) {
            return;
        }
        synchronized (priorityTasks) {
            for (Queue<Object> queue : priorityTasks.values()) {
                List<Runnable> runnableList = new LinkedList<>();
                List<CallableSupplier<Object>> callableSupplierList = new LinkedList<>();
                while (!queue.isEmpty()) {
                    Object element = queue.poll();
                    if (element instanceof Runnable) {
                        runnableList.add((Runnable) element);
                    } else if (element instanceof CallableSupplier) {
                        callableSupplierList.add((CallableSupplier<Object>) element);
                    }
                }
                super.runAsync(runnableList);
                super.supplyCallableAsync(callableSupplierList);
            }
        }
    }

    public void start() {
        consumeService.start();
    }

    @Override
    public void shutdown() {
        if (!consumeService.isStopped()) {
            consumeService.shutdown();
        }
        super.shutdown();
    }

    @Override
    public void shutdown(long awaitTerminateMillis) {
        if (!consumeService.isStopped()) {
            consumeService.setJoinTime(awaitTerminateMillis);
            consumeService.shutdown();
        }
        super.shutdown(awaitTerminateMillis);
    }
}
