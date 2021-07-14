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
package org.apache.rocketmq.common.protocol.body;

import java.util.List;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class BatchCreateTopicRequestBody extends RemotingSerializable {
    @CFNotNull
    private List<TopicConfig> topicConfigList;
    @CFNotNull
    private String defaultTopic;

    public List<TopicConfig> getTopicConfigList() {
        return topicConfigList;
    }

    public void setTopicConfigList(List<TopicConfig> topicConfigList) {
        this.topicConfigList = topicConfigList;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }
}
