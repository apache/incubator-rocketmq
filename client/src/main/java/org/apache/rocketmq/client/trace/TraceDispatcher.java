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

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import java.io.IOException;

/**
 * Interface of asynchronous transfer data
 */
public interface TraceDispatcher {
    enum Type {
        PRODUCE,
        CONSUME
    }
    /**
     * Initialize asynchronous transfer data module
     */
    void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException;

    /**
     * Append the transfering data
     * @param ctx data infomation
     * @return
     */
    boolean append(Object ctx);

    /**
     * Write flush action
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close the trace Hook
     */
    void shutdown();
}
