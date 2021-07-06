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
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TraceProducerByJaeger {

    private  final Tracer tracer;

    public TraceProducerByJaeger(Tracer tracer) {
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

    private  void send(Span rootSpan) {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName",tracer,rootSpan);
        producer.setNamesrvAddr("127.0.0.1:9876");
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 10; i++)
            try {
                {
                    Message msg = new Message("test",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.shutdown();
    }


    public static JaegerTracer init(String service) {
        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1);
        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration config = new Configuration(service).withSampler(samplerConfig).withReporter(reporterConfig);
        return config.getTracer();
    }

    public static void main(String[] args) throws MQClientException, InterruptedException {
        JaegerTracer tracer = init("traceProducerByJaeger");
        Span span = tracer.buildSpan("rootSpan").start();
        TraceProducerByJaeger traceProducerByJaeger = new TraceProducerByJaeger(tracer);
        traceProducerByJaeger.printString(span,"hello");
        traceProducerByJaeger.send(span);
        span.finish();
    }
}
