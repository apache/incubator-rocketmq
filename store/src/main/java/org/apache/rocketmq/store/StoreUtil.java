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
package org.apache.rocketmq.store;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;

public class StoreUtil {
    public static final long TOTAL_PHYSICAL_MEMORY_SIZE = getTotalPhysicalMemorySize();

    @SuppressWarnings("restriction")
    public static long getTotalPhysicalMemorySize() {
        long physicalTotal = 1024 * 1024 * 1024 * 24L;
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
            physicalTotal = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
        }

        return physicalTotal;
    }

    public static List<String> getCommitLogStorePaths(String storePathCommitLog) {
        List<String> storPaths = null;
        if (!UtilAll.isBlank(storePathCommitLog)) {
            storPaths = Arrays.asList(storePathCommitLog.trim().split(";"));
        }
        return storPaths;
    }

    public static List<String> getReadOnlyStorePaths(String readOnlyCommitLogStorePaths) {
        List<String> storPaths = null;
        if (!UtilAll.isBlank(readOnlyCommitLogStorePaths)) {
            storPaths = Arrays.asList(readOnlyCommitLogStorePaths.trim().split(";"));
        }
        return storPaths;
    }
}
