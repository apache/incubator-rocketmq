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

package org.apache.rocketmq.oms.api.impl.rocketmq;

public class FAQ {
    public static final String FIND_NS_FAILED =
            "http://rocketmq.apache.org/docs/faq/exceptions&namesrv_not_exist";

    public static final String CONNECT_BROKER_FAILED =
            "http://rocketmq.apache.org/docs/faq/exceptions&connect_broker_failed";

    public static final String SEND_MSG_TO_BROKER_TIMEOUT =
            "http://rocketmq.apache.org/docs/faq/exceptions&send_msg_failed";

    public static final String SERVICE_STATE_WRONG =
            "http://rocketmq.apache.org/docs/faq/exceptions&service_not_ok";

    public static final String BROKER_RESPONSE_EXCEPTION =
            "http://rocketmq.apache.org/docs/faq/exceptions&broker_response_exception";

    public static final String CLIENT_CHECK_MSG_EXCEPTION =
            "http://rocketmq.apache.org/docs/faq/exceptions&msg_check_failed";

    public static final String TOPIC_ROUTE_NOT_EXIST =
            "http://rocketmq.apache.org/docs/faq/exceptions&topic_not_exist";


    public static String errorMessage(final String msg, final String url) {
        return String.format("%s\nSee %s for further details.", msg, url);
    }
}
