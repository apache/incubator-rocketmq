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
package org.apache.rocketmq.srvutil;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class ConcurrentHashMapUtil {
    private static final boolean IS_JDK8;

    static {
        // Java 8 or lower: 1.6.0_23, 1.7.0, 1.7.0_80, 1.8.0_211
        // Java 9 or higher: 9.0.1, 11.0.4, 12, 12.0.1
        IS_JDK8 = System.getProperty("java.version").startsWith("1.8.");
    }

    private ConcurrentHashMapUtil() {
    }

    /**
     * A temporary workaround for Java 8 specific performance issue JDK-8161372 .<br> Use implementation of
     * ConcurrentMap.computeIfAbsent instead.
     *
     * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8161372">https://bugs.openjdk.java.net/browse/JDK-8161372</a>
     */
    public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Function<? super K, ? extends V> func) {
        if (IS_JDK8) {
            V v, newValue;
            return ((v = map.get(key)) == null &&
                (newValue = func.apply(key)) != null &&
                (v = map.putIfAbsent(key, newValue)) == null) ? newValue : v;
        } else {
            return map.computeIfAbsent(key, func);
        }
    }
}
