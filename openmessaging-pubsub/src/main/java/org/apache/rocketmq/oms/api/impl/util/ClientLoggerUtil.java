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
 * See the License for the specific language governing permissioms and
 * limitatioms under the License.
 */
package org.apache.rocketmq.oms.api.impl.util;

import java.util.Arrays;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;

public class ClientLoggerUtil {
    private static final String CLIENT_LOG_ROOT = "oms.client.logRoot";
    private static final String CLIENT_LOG_FILEMAXINDEX = "oms.client.logFileMaxIndex";
    private static final int CLIENT_LOG_FILE_MAX_INDEX = 100;
    private static final String CLIENT_LOG_LEVEL = "oms.client.logLevel";
    private static final String[] LEVEL_ARRAY = {"ERROR", "WARN", "INFO", "DEBUG"};
    private static final long CLIENT_LOG_FILESIZE = 64 * 1024 * 1024L;

    public static InternalLogger getClientLogger() {
        //Make sure
        String omsClientLogRoot = System.getProperty(CLIENT_LOG_ROOT, System.getProperty("user.home") + "/logs");
        System.setProperty(ClientLogger.CLIENT_LOG_ROOT, omsClientLogRoot);
        String omsClientLogLevel = System.getProperty(CLIENT_LOG_LEVEL, "INFO").trim().toUpperCase();
        if (!Arrays.asList(LEVEL_ARRAY).contains(omsClientLogLevel)) {
            omsClientLogLevel = "INFO";
        }
        System.setProperty(ClientLogger.CLIENT_LOG_LEVEL, omsClientLogLevel);
        String omsClientLogMaxIndex = System.getProperty(CLIENT_LOG_FILEMAXINDEX, "10").trim();
        try {
            int maxIndex = Integer.parseInt(omsClientLogMaxIndex);
            if (maxIndex <= 0 || maxIndex > CLIENT_LOG_FILE_MAX_INDEX) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            omsClientLogMaxIndex = "10";
        }
        System.setProperty(ClientLogger.CLIENT_LOG_MAXINDEX, omsClientLogMaxIndex);
        System.setProperty(ClientLogger.CLIENT_LOG_FILENAME, "openmessaging.log");
        System.setProperty(ClientLogger.CLIENT_LOG_FILESIZE, String.valueOf(CLIENT_LOG_FILESIZE));
        return ClientLogger.getLog();
    }
}
