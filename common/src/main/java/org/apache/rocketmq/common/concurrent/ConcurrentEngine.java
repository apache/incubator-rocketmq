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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class ConcurrentEngine {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    protected final ExecutorService enginePool;

    public ConcurrentEngine() {
        this.enginePool = ForkJoinPool.commonPool();
    }

    public ConcurrentEngine(ExecutorService enginePool) {
        this.enginePool = enginePool;
    }

    public final void runAsync(Runnable... tasks) {
        runAsync(UtilAll.newArrayList(tasks));
    }

    protected static <E> List<E> pollAllTask(Queue<E> tasks) {
        //avoid list expansion
        List<E> list = new LinkedList<>();
        while (tasks != null && !tasks.isEmpty()) {
            E task = tasks.poll();
            list.add(task);
        }
        return list;
    }

    protected static <T> void doCallback(CallableSupplier<T> supplier, T response) {
        Collection<Callback<T>> callbacks = supplier.getCallbacks();
        if (CollectionUtils.isNotEmpty(callbacks)) {
            for (Callback<T> callback : callbacks) {
                callback.call(response);
            }
        }
    }

    public final void runAsync(Queue<? extends Runnable> tasks) {
        runAsync(pollAllTask(tasks));
    }

    public final void runAsync(Collection<? extends Runnable> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return;
        }
        List<CompletableFuture<Void>> list = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            list.add(CompletableFuture.runAsync(task, enginePool));
        }
        executeAsync(list);
    }

    @SafeVarargs
    public final <T> List<T> supplyAsync(Supplier<T>... tasks) {
        return supplyAsync(UtilAll.newArrayList(tasks));
    }

    public final <T> List<T> supplyAsync(Queue<? extends Supplier<T>> tasks) {
        return supplyAsync(pollAllTask(tasks));
    }

    public final <T> List<T> supplyAsync(Collection<? extends Supplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new ArrayList<>();
        }
        List<CompletableFuture<T>> list = new ArrayList<>(tasks.size());
        for (Supplier<T> task : tasks) {
            list.add(CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeAsync(list);
    }

    @SafeVarargs
    public final <T> List<T> supplyCallableAsync(CallableSupplier<T>... tasks) {
        return supplyCallableAsync(UtilAll.newArrayList(tasks));
    }

    public final <T> List<T> supplyCallableAsync(Queue<? extends CallableSupplier<T>> tasks) {
        return supplyCallableAsync(pollAllTask(tasks));
    }

    public final <T> List<T> supplyCallableAsync(Collection<? extends CallableSupplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new ArrayList<>();
        }
        Map<CallableSupplier<T>, CompletableFuture<T>> map = new HashMap<>(tasks.size());
        for (CallableSupplier<T> task : tasks) {
            map.put(task, CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<CallableSupplier<T>, T> result = executeKeyedAsync(map);
        for (Map.Entry<CallableSupplier<T>, T> entry : result.entrySet()) {
            doCallback(entry.getKey(), entry.getValue());
        }
        return UtilAll.newArrayList(result.values());
    }

    @SafeVarargs
    public final <K, V> Map<K, V> supplyKeyedCallableAsync(KeyedCallableSupplier<K, V>... tasks) {
        return supplyKeyedCallableAsync(UtilAll.newArrayList(tasks));
    }

    public final <K, V> Map<K, V> supplyKeyedCallableAsync(Queue<? extends KeyedCallableSupplier<K, V>> tasks) {
        return supplyKeyedCallableAsync(pollAllTask(tasks));
    }

    public final <K, V> Map<K, V> supplyKeyedCallableAsync(Collection<? extends KeyedCallableSupplier<K, V>> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new HashMap<>();
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedCallableSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<K, V> result = executeKeyedAsync(map);
        for (KeyedCallableSupplier<K, V> task : tasks) {
            K key = task.key();
            V response = result.get(key);
            doCallback(task, response);
        }
        return result;
    }

    @SafeVarargs
    public final <T> List<T> executeAsync(CompletableFuture<T>... tasks) {
        return executeAsync(UtilAll.newArrayList(tasks));
    }

    public final <T> List<T> executeAsync(Queue<CompletableFuture<T>> tasks) {
        return executeAsync(pollAllTask(tasks));
    }

    public final <T> List<T> executeAsync(Collection<CompletableFuture<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getResultIgnoreException(tasks);
    }

    public final <T> List<T> getResultIgnoreException(Collection<CompletableFuture<T>> tasks) {
        List<T> result = new ArrayList<>(tasks.size());
        for (CompletableFuture<T> completableFuture : tasks) {
            if (null == completableFuture) {
                continue;
            }
            try {
                T response = completableFuture.get();
                if (null != response) {
                    result.add(response);
                }
            } catch (Exception e) {
                log.error("task:{} execute failed with exception:{}", completableFuture, e.getMessage(), e);
            }
        }
        return result;
    }

    public final void runAsync(long timeout, TimeUnit unit, Runnable... tasks) {
        runAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public final void runAsync(long timeout, TimeUnit unit, Queue<? extends Runnable> tasks) {
        runAsync(timeout, unit, pollAllTask(tasks));
    }

    public final void runAsync(long timeout, TimeUnit unit, Collection<? extends Runnable> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return;
        }
        List<CompletableFuture<Void>> list = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            list.add(CompletableFuture.runAsync(task, enginePool));
        }
        executeAsync(timeout, unit, list);
    }

    @SafeVarargs
    public final <T> List<T> supplyAsync(long timeout, TimeUnit unit, Supplier<T>... tasks) {
        return supplyAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public final <T> List<T> supplyAsync(long timeout, TimeUnit unit, Queue<? extends Supplier<T>> tasks) {
        return supplyAsync(timeout, unit, pollAllTask(tasks));
    }

    public final <T> List<T> supplyAsync(long timeout, TimeUnit unit, Collection<? extends Supplier<T>> tasks) {
        if (null == tasks || tasks.size() == 0 || enginePool.isShutdown()) {
            return new ArrayList<>();
        }
        List<CompletableFuture<T>> list = new ArrayList<>(tasks.size());
        for (Supplier<T> task : tasks) {
            list.add(CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeAsync(timeout, unit, list);
    }

    @SafeVarargs
    public final <T> List<T> supplyCallableAsync(long timeout, TimeUnit unit, CallableSupplier<T>... tasks) {
        return supplyCallableAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public final <T> List<T> supplyCallableAsync(long timeout, TimeUnit unit,
        Queue<? extends CallableSupplier<T>> tasks) {
        return supplyCallableAsync(timeout, unit, pollAllTask(tasks));
    }

    public final <T> List<T> supplyCallableAsync(long timeout, TimeUnit unit,
        Collection<? extends CallableSupplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new ArrayList<>();
        }
        Map<CallableSupplier<T>, CompletableFuture<T>> map = new HashMap<>(tasks.size());
        for (CallableSupplier<T> task : tasks) {
            map.put(task, CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<CallableSupplier<T>, T> result = executeKeyedAsync(map, timeout, unit);
        for (Map.Entry<CallableSupplier<T>, T> entry : result.entrySet()) {
            doCallback(entry.getKey(), entry.getValue());
        }
        return UtilAll.newArrayList(result.values());
    }

    @SafeVarargs
    public final <K, V> Map<K, V> supplyKeyedCallableAsync(long timeout, TimeUnit unit,
        KeyedCallableSupplier<K, V>... tasks) {
        return supplyKeyedCallableAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public final <K, V> Map<K, V> supplyKeyedCallableAsync(long timeout, TimeUnit unit,
        Queue<? extends KeyedCallableSupplier<K, V>> tasks) {
        return supplyKeyedCallableAsync(timeout, unit, pollAllTask(tasks));
    }

    public final <K, V> Map<K, V> supplyKeyedCallableAsync(long timeout, TimeUnit unit,
        Collection<? extends KeyedCallableSupplier<K, V>> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new HashMap<>();
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedCallableSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<K, V> result = executeKeyedAsync(map, timeout, unit);
        for (KeyedCallableSupplier<K, V> task : tasks) {
            K key = task.key();
            V response = result.get(key);
            doCallback(task, response);
        }
        return result;
    }

    @SafeVarargs
    public final <T> List<T> executeAsync(long timeout, TimeUnit unit, CompletableFuture<T>... tasks) {
        return executeAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public final <T> List<T> executeAsync(long timeout, TimeUnit unit, Queue<CompletableFuture<T>> tasks) {
        return executeAsync(timeout, unit, pollAllTask(tasks));
    }

    public final <T> List<T> executeAsync(long timeout, TimeUnit unit, Collection<CompletableFuture<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getResultIgnoreException(tasks, timeout, unit);
    }

    public static <T> List<T> getResultIgnoreException(Collection<CompletableFuture<T>> tasks, long timeout,
        TimeUnit unit) {
        List<T> result = new ArrayList<>(tasks.size());
        for (CompletableFuture<T> completableFuture : tasks) {
            if (null == completableFuture) {
                continue;
            }
            try {
                T response = completableFuture.get(timeout, unit);
                if (null != response) {
                    result.add(response);
                }
            } catch (Exception e) {
                log.error("task:{} execute failed with exception:{}", completableFuture, e.getMessage(), e);
            }
        }
        return result;
    }

    @SafeVarargs
    public final <K, V> Map<K, V> supplyKeyedAsync(KeyedSupplier<K, V>... tasks) {
        return supplyKeyedAsync(UtilAll.newArrayList(tasks));
    }

    public final <K, V> Map<K, V> supplyKeyedAsync(Queue<? extends KeyedSupplier<K, V>> tasks) {
        return supplyKeyedAsync(pollAllTask(tasks));
    }

    public final <K, V> Map<K, V> supplyKeyedAsync(Collection<? extends KeyedSupplier<K, V>> tasks) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new HashMap<>(0);
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeKeyedAsync(map);
    }

    public static <K, V> Map<K, V> executeKeyedAsync(Map<K, CompletableFuture<V>> tasks) {
        if (MapUtils.isEmpty(tasks)) {
            return new HashMap<>(0);
        }
        try {
            CompletableFuture.allOf(tasks.values().toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getKeyedResultIgnoreException(tasks);
    }

    public static <K, V> Map<K, V> getKeyedResultIgnoreException(Map<K, CompletableFuture<V>> tasks) {
        Map<K, V> result = new HashMap<>(tasks.size());
        for (Map.Entry<K, CompletableFuture<V>> entry : tasks.entrySet()) {
            K key = entry.getKey();
            CompletableFuture<V> value = entry.getValue();
            if (null == value) {
                continue;
            }
            try {
                V response = value.get();
                if (null != response) {
                    result.put(key, response);
                }
            } catch (Exception e) {
                log.error("task with key:{} execute failed with exception:{}", key, e.getMessage(), e);
            }
        }
        return result;
    }

    @SafeVarargs
    public final <K, V> Map<K, V> supplyKeyedAsync(long timeout, TimeUnit unit, KeyedSupplier<K, V>... tasks) {
        return supplyKeyedAsync(UtilAll.newArrayList(tasks), timeout, unit);
    }

    public final <K, V> Map<K, V> supplyKeyedAsync(long timeout, TimeUnit unit,
        Queue<? extends KeyedSupplier<K, V>> tasks) {
        return supplyKeyedAsync(pollAllTask(tasks), timeout, unit);
    }

    public final <K, V> Map<K, V> supplyKeyedAsync(Collection<? extends KeyedSupplier<K, V>> tasks, long timeout,
        TimeUnit unit) {
        if (CollectionUtils.isEmpty(tasks) || enginePool.isShutdown()) {
            return new HashMap<>(0);
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeKeyedAsync(map, timeout, unit);
    }

    public static <K, V> Map<K, V> executeKeyedAsync(Map<K, CompletableFuture<V>> tasks, long timeout, TimeUnit unit) {
        if (MapUtils.isEmpty(tasks)) {
            return new HashMap<>(0);
        }
        try {
            CompletableFuture.allOf(tasks.values().toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getKeyedResultIgnoreException(tasks, timeout, unit);
    }

    public static <K, V> Map<K, V> getKeyedResultIgnoreException(Map<K, CompletableFuture<V>> tasks, long timeout,
        TimeUnit unit) {
        Map<K, V> result = new HashMap<>(tasks.size());
        for (Map.Entry<K, CompletableFuture<V>> entry : tasks.entrySet()) {
            K key = entry.getKey();
            CompletableFuture<V> value = entry.getValue();
            if (null == value) {
                continue;
            }
            try {
                V response = value.get(timeout, unit);
                if (null != response) {
                    result.put(key, response);
                }
            } catch (Exception e) {
                log.error("task with key:{} execute failed with exception:{}", key, e.getMessage(), e);
            }
        }
        return result;
    }

    public void shutdown() {
        if (enginePool != null && !enginePool.isShutdown()) {
            ThreadUtils.shutdownGracefully(enginePool, 0, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown(long awaitTerminateMillis) {
        if (enginePool != null && !enginePool.isShutdown()) {
            ThreadUtils.shutdownGracefully(enginePool, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        }
    }

    public boolean isShutdown() {
        if (null == enginePool) {
            return true;
        }
        return enginePool.isShutdown();
    }
}
