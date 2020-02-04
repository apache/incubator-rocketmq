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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageType;

public class TraceBean {
    private static final String LOCAL_ADDRESS = UtilAll.ipToIPv4Str(UtilAll.getIP());
    private String topic = "";
    private String msgId = "";
    private String offsetMsgId = "";
    private String tags = "";
    private String keys = "";
    private String storeHost = LOCAL_ADDRESS;
    private String clientHost = LOCAL_ADDRESS;
    private long storeTime;
    private int retryTimes;
    private int bodyLength;
    private MessageType msgType;


    public MessageType getMsgType() {
        return msgType;
    }


    public void setMsgType(final MessageType msgType) {
        this.msgType = msgType;
    }


    public String getOffsetMsgId() {
        return offsetMsgId;
    }


    public void setOffsetMsgId(final String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }

    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public String getTags() {
        return tags;
    }


    public void setTags(String tags) {
        this.tags = tags;
    }


    public String getKeys() {
        return keys;
    }


    public void setKeys(String keys) {
        this.keys = keys;
    }


    public String getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(String storeHost) {
        this.storeHost = storeHost;
    }


    public String getClientHost() {
        return clientHost;
    }


    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }


    public long getStoreTime() {
        return storeTime;
    }


    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }


    public int getRetryTimes() {
        return retryTimes;
    }


    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }


    public int getBodyLength() {
        return bodyLength;
    }


    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }
}
