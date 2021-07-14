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

/**
 * $Id: QueryConsumerOffsetResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryConsumerStageOffsetResponseHeader implements CommandCustomHeader {
    @CFNullable
    private Map<String, Map<String, Integer>> stageOffset = new HashMap<>();

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Map<String, Map<String, Integer>> getStageOffset() {
        return stageOffset;
    }

    public void setStageOffset(Map<String, Map<String, Integer>> stageOffset) {
        this.stageOffset = stageOffset;
    }
}
