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

package org.apache.rocketmq.client.producer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RequestResponseFutureTest {

    @Test
    public void testExecuteRequestCallback() throws Exception {
        final AtomicInteger cc = new AtomicInteger(0);
        RequestResponseFuture future = new RequestResponseFuture(UUID.randomUUID().toString(), 3 * 1000L, new RequestCallback() {
            @Override public void onSuccess(Message message) {
                cc.incrementAndGet();
            }

            @Override public void onException(Throwable e) {
            }
        });
        future.setSendReqeustOk(true);
        future.executeRequestCallback();
        assertThat(cc.get()).isEqualTo(1);
    }

}
