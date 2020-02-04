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
package org.apache.rocketmq.acl.common;

public class AclConstants {

    public static final String CONFIG_GLOBAL_WHITE_ADDRS = "globalWhiteRemoteAddresses";

    public static final String CONFIG_ACCOUNTS = "accounts";

    public static final String CONFIG_ACCESS_KEY = "accessKey";

    public static final String CONFIG_SECRET_KEY = "secretKey";

    public static final String CONFIG_WHITE_ADDR = "whiteRemoteAddress";

    public static final String CONFIG_ADMIN_ROLE = "admin";

    public static final String CONFIG_DEFAULT_TOPIC_PERM = "defaultTopicPerm";

    public static final String CONFIG_DEFAULT_GROUP_PERM = "defaultGroupPerm";

    public static final String CONFIG_TOPIC_PERMS = "topicPerms";

    public static final String CONFIG_GROUP_PERMS = "groupPerms";

    public static final String CONFIG_DATA_VERSION = "dataVersion";

    public static final String CONFIG_COUNTER = "counter";

    public static final String CONFIG_TIME_STAMP = "timestamp";

    public static final int ACCESS_KEY_MIN_LENGTH = 6;

    public static final int SECRET_KEY_MIN_LENGTH = 6;
}
