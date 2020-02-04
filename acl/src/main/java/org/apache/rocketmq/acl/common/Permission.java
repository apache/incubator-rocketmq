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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import org.apache.rocketmq.common.protocol.RequestCode;

public class Permission {

    public static final byte DENY = 1;
    public static final byte ANY = 1 << 1;
    public static final byte PUB = 1 << 2;
    public static final byte SUB = 1 << 3;

    public static final Set<Integer> ADMIN_CODE = new HashSet<Integer>();

    static {
        // UPDATE_AND_CREATE_TOPIC
        ADMIN_CODE.add(RequestCode.UPDATE_AND_CREATE_TOPIC);
        // UPDATE_BROKER_CONFIG
        ADMIN_CODE.add(RequestCode.UPDATE_BROKER_CONFIG);
        // DELETE_TOPIC_IN_BROKER
        ADMIN_CODE.add(RequestCode.DELETE_TOPIC_IN_BROKER);
        // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
        ADMIN_CODE.add(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
        // DELETE_SUBSCRIPTIONGROUP
        ADMIN_CODE.add(RequestCode.DELETE_SUBSCRIPTIONGROUP);
    }

    public static boolean checkPermission(byte neededPerm, byte ownedPerm) {
        if ((ownedPerm & DENY) > 0) {
            return false;
        }
        if ((neededPerm & ANY) > 0) {
            return ((ownedPerm & PUB) > 0) || ((ownedPerm & SUB) > 0);
        }
        return (neededPerm & ownedPerm) > 0;
    }

    public static byte parsePermFromString(String permString) {
        if (permString == null) {
            return Permission.DENY;
        }
        switch (permString.trim()) {
            case "PUB":
                return Permission.PUB;
            case "SUB":
                return Permission.SUB;
            case "PUB|SUB":
                return Permission.PUB | Permission.SUB;
            case "SUB|PUB":
                return Permission.PUB | Permission.SUB;
            case "DENY":
                return Permission.DENY;
            default:
                return Permission.DENY;
        }
    }

    public static void parseResourcePerms(PlainAccessResource plainAccessResource, Boolean isTopic,
        List<String> resources) {
        if (resources == null || resources.isEmpty()) {
            return;
        }
        for (String resource : resources) {
            String[] items = StringUtils.split(resource, "=");
            if (items.length == 2) {
                plainAccessResource.addResourceAndPerm(isTopic ? items[0].trim() : PlainAccessResource.getRetryTopic(items[0].trim()), parsePermFromString(items[1].trim()));
            } else {
                throw new AclException(String.format("Parse resource permission failed for %s:%s", isTopic ? "topic" : "group", resource));
            }
        }
    }

    public static boolean needAdminPerm(Integer code) {
        return ADMIN_CODE.contains(code);
    }
}
