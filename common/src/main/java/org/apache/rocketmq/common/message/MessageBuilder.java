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
package org.apache.rocketmq.common.message;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

// user facede of message to send, less pressure about message.
public class MessageBuilder {

    private String topic;
    private Map<String, String> properties;
    private byte[] body;

    public MessageBuilder() {
    }

    public MessageBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public MessageBuilder withBody(byte[] body) {
        this.body = body;
        return this;
    }

    private MessageBuilder withProperty(final String key, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<String, String>();
        }

        this.properties.put(key, value);
        return this;
    }

    public MessageBuilder withKeys(String keys) {
        return this.withProperty(MessageConst.PROPERTY_KEYS, keys);
    }

    public MessageBuilder withKeys(Collection<String> keys) {
        StringBuffer sb = new StringBuffer();
        for (String k : keys) {
            sb.append(k);
            sb.append(MessageConst.KEY_SEPARATOR);
        }

        return this.withKeys(sb.toString().trim());
    }

    public MessageBuilder withDelayTimeLevel(int level) {
        return this.withProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }

    public MessageBuilder withTags(String tags) {
        return this.withProperty(MessageConst.PROPERTY_TAGS, tags);
    }

    public MessageBuilder withUserProperty(String key, String value) {
        if (MessageConst.STRING_HASH_SET.contains(key)) {
            throw new RuntimeException(String.format(
                "The Property<%s> is used by system, input another please", key));
        }

        if (value == null || value.trim().isEmpty()
            || key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "The name or value of property can not be null or blank string!"
            );
        }

        return this.withProperty(key, value);
    }

    public Message build() {
        Message msg = new Message(topic, body);
        if (this.properties != null) {
            for (Map.Entry<String, String> entry : this.properties.entrySet()) {
                msg.putProperty(entry.getKey(), entry.getValue());
            }
        }
        return msg;
    }
}
