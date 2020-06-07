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

package org.apache.rocketmq.oms.api.impl;

import java.io.InputStream;
import java.util.Properties;
import org.apache.rocketmq.common.MQVersion;

public class MQClientInfo {

    public static int versionCode = MQVersion.CURRENT_VERSION;
    public static String currentVersion;

    static {
        try {
            InputStream stream = MQClientInfo.class.getClassLoader().getResourceAsStream("ons_client_info.properties");
            Properties properties = new Properties();
            properties.load(stream);
            currentVersion = String.valueOf(properties.get("version"));
            versionCode = Integer.MAX_VALUE - Integer.valueOf(currentVersion.replaceAll("[^0-9]", ""));
        } catch (Exception ignore) {
        }
    }

}
