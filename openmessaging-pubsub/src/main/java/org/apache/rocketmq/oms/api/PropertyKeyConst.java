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
package org.apache.rocketmq.oms.api;

import io.openmessaging.api.OMSBuiltinKeys;

public class PropertyKeyConst implements OMSBuiltinKeys {

    public static final String MESSAGE_MODEL = "MessageModel";

    public static final String GROUP_ID = "GROUP_ID";

    public static final String ACCESS_KEY = "AccessKey";

    public static final String SECRET_KEY = "SecretKey";

    public static final String SECURITY_TOKEN = "SecurityToken";

    public static final String SEND_MSG_TIMEOUT_MILLIS = "SendMsgTimeoutMillis";

    public static final String ONS_ADDR = "ONSAddr";

    public static final String NAMESRV_ADDR = "NAMESRV_ADDR";

    public static final String CONSUME_THREAD_NUMS = "ConsumeThreadNums";

    public static final String MQ_TYPE = "MQType";

    public static final String VIP_CHANNEL_ENABLED = "isVipChannelEnabled";

    public static final String SUSPEND_TIME_MILLIS = "suspendTimeMillis";

    public static final String MAX_RECONSUME_TIMES = "maxReconsumeTimes";

    public static final String CONSUME_TIMEOUT = "consumeTimeout";

    public static final String CHECK_IMMUNITY_TIME_IN_SECONDS = "CheckImmunityTimeInSeconds";

    public static final String POST_SUBSCRIPTION_WHEN_PULL = "PostSubscriptionWhenPull";

    public static final String CONSUME_MESSAGE_BATCH_MAX_SIZE = "ConsumeMessageBatchMaxSize";

    public static final String MAX_CACHED_MESSAGE_AMOUNT = "maxCachedMessageAmount";

    public static final String MAX_CACHED_MESSAGE_SIZE_IN_MB = "maxCachedMessageSizeInMiB";

    public static final String INSTANCE_NAME = "InstanceName";

    public static final String QOS = "qos";

    public static final String MAX_BATCH_MESSAGE_COUNT = "maxBatchMessageCount";

    public static final String INSTANCE_ID = "instanceId";

    public static final String LANGUAGE_IDENTIFIER = "languageIdentifier";

    public static final String MSG_TRACE_SWITCH = "msgTraceSwitch";

    public static final String AUTO_COMMIT = "autoCommit";

    public static final String ACCESS_CHANNEL = "accessChannel";

    public static final String TRACE_TOPIC_NAME = "traceTopicName";

    public static final String ACL_ENABLE = "aclEnable";

}
