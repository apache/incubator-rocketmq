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
package org.apache.rocketmq.example.opentracing;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

public class TraceConsumerByJaeger {

    private final Tracer tracer;

    public TraceConsumerByJaeger(Tracer tracer) {
        this.tracer = tracer;
    }


    private  void printString(Span rootSpan, String helloStr) {
        Span span = tracer.buildSpan("printStringSpan").asChildOf(rootSpan).start();
        try {
            System.out.printf(helloStr);
            span.log("event:print");
        } finally {
            span.finish();
        }
    }

    private void consumerMessage(Span rootSpan) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerGroupName",  tracer,rootSpan);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeMessageBatchMaxSize(3);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("test", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    public static JaegerTracer init(String service) {
        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1);
        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration config = new Configuration(service).withSampler(samplerConfig).withReporter(reporterConfig);
        return config.getTracer();
    }


    public static void main(String[] args) throws MQClientException, InterruptedException {
        JaegerTracer tracer = init("traceConsumerByJaeger");
        Span span = tracer.buildSpan("rootSpan").start();
        TraceConsumerByJaeger traceConsumerByJaeger = new TraceConsumerByJaeger(tracer);
        traceConsumerByJaeger.printString(span,"hello");
        traceConsumerByJaeger.consumerMessage(span);
        span.finish();
    }
}
