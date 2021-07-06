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
package org.apache.rocketmq.client.opentracing.hook;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.message.MessageExt;

public class ConsumeMessageOpenTracingHookImpl  implements ConsumeMessageHook {
    private Span rootSpan;
    private Span span;
    private Tracer tracer;

    public ConsumeMessageOpenTracingHookImpl(Span rootSpan,Tracer tracer) {
        this.rootSpan = rootSpan;
        this.tracer = tracer;
    }

    @Override
    public String hookName() {
        return "ConsumeMessageOpenTracingHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        span = tracer.buildSpan("RocketMQConsumer").withTag(Tags.SPAN_KIND.getKey(),Tags.SPAN_KIND_CONSUMER).asChildOf(rootSpan).start();
    }


    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        for (MessageExt ext:context.getMsgList()) {
            Map<String, String> map = new HashMap();
            map.put("topic:",ext.getTopic());
            map.put("msgId:", ext.getMsgId());
            map.put("tag:", ext.getTags());
            map.put("key:", ext.getKeys());
            map.put("bodyLength:", String.valueOf(ext.getBody().length));
            span.log(map);
        }
        span.finish();
    }
}
