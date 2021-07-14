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
import java.util.function.Supplier;

/**
 * To make up for the pain point of the original {@link Supplier} without callback
 */
public interface CallableSupplier<T> extends Supplier<T> {

    default Callback<T> getCallback() {
        return null;
    }

    default Collection<Callback<T>> getCallbacks() {
        ArrayList<Callback<T>> callbacks = new ArrayList<>();
        Callback<T> callback = getCallback();
        if (callback != null) {
            callbacks.add(callback);
        }
        return callbacks;
    }

    default boolean addCallback(Callback<T> callback) {
        Collection<Callback<T>> callbacks = getCallbacks();
        if (null == callbacks) {
            return false;
        }
        return callbacks.add(callback);
    }

    default boolean removeCallback(Callback<T> callback) {
        Collection<Callback<T>> callbacks = getCallbacks();
        if (null == callbacks) {
            return true;
        }
        return callbacks.remove(callback);
    }
}
