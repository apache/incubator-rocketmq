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

package org.apache.rocketmq.example.benchmark;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.srvutil.ServerUtil;

public class Consumer {

    public static void main(String[] args) throws MQClientException, IOException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkConsumer", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final String groupPrefix = commandLine.hasOption('g') ? commandLine.getOptionValue('g').trim() : "benchmark_consumer";
        final String isSuffixEnable = commandLine.hasOption('p') ? commandLine.getOptionValue('p').trim() : "true";
        final String filterType = commandLine.hasOption('f') ? commandLine.getOptionValue('f').trim() : null;
        final String expression = commandLine.hasOption('e') ? commandLine.getOptionValue('e').trim() : null;
        final double failRate = commandLine.hasOption('r') ? Double.parseDouble(commandLine.getOptionValue('r').trim()) : 0.0;
        String group = groupPrefix;
        if (Boolean.parseBoolean(isSuffixEnable)) {
            group = groupPrefix + "_" + (System.currentTimeMillis() % 100);
        }

        System.out.printf("topic: %s, group: %s, suffix: %s, filterType: %s, expression: %s%n", topic, group, isSuffixEnable, filterType, expression);

        final StatsBenchmarkConsumer statsBenchmarkConsumer = new StatsBenchmarkConsumer();

        final Timer timer = new Timer("BenchmarkTimerThread", true);

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmarkConsumer.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long consumeTps =
                        (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageB2CRT = (end[2] - begin[2]) / (double) (end[1] - begin[1]);
                    final double averageS2CRT = (end[3] - begin[3]) / (double) (end[1] - begin[1]);
                    final long failCount = end[4] - begin[4];
                    final long b2cMax = statsBenchmarkConsumer.getBorn2ConsumerMaxRT().get();
                    final long s2cMax = statsBenchmarkConsumer.getStore2ConsumerMaxRT().get();

                    statsBenchmarkConsumer.getBorn2ConsumerMaxRT().set(0);
                    statsBenchmarkConsumer.getStore2ConsumerMaxRT().set(0);

                    System.out.printf("TPS: %d FAIL: %d AVG(B2C) RT: %7.3f AVG(S2C) RT: %7.3f MAX(B2C) RT: %d MAX(S2C) RT: %d%n",
                            consumeTps, failCount, averageB2CRT, averageS2CRT, b2cMax, s2cMax
                    );
                }
            }

            @Override
            public void run() {
                try {
                    this.printStats();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10000, 10000);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            consumer.setNamesrvAddr(ns);
        }
        consumer.setInstanceName(Long.toString(System.currentTimeMillis()));

        if (filterType == null || expression == null) {
            consumer.subscribe(topic, "*");
        } else {
            if (ExpressionType.TAG.equals(filterType)) {
                String expr = MixAll.file2String(expression);
                System.out.printf("Expression: %s%n", expr);
                consumer.subscribe(topic, MessageSelector.byTag(expr));
            } else if (ExpressionType.SQL92.equals(filterType)) {
                String expr = MixAll.file2String(expression);
                System.out.printf("Expression: %s%n", expr);
                consumer.subscribe(topic, MessageSelector.bySql(expr));
            } else {
                throw new IllegalArgumentException("Not support filter type! " + filterType);
            }
        }

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                MessageExt msg = msgs.get(0);
                long now = System.currentTimeMillis();

                statsBenchmarkConsumer.getReceiveMessageTotalCount().incrementAndGet();

                long born2ConsumerRT = now - msg.getBornTimestamp();
                statsBenchmarkConsumer.getBorn2ConsumerTotalRT().addAndGet(born2ConsumerRT);

                long store2ConsumerRT = now - msg.getStoreTimestamp();
                statsBenchmarkConsumer.getStore2ConsumerTotalRT().addAndGet(store2ConsumerRT);

                compareAndSetMax(statsBenchmarkConsumer.getBorn2ConsumerMaxRT(), born2ConsumerRT);

                compareAndSetMax(statsBenchmarkConsumer.getStore2ConsumerMaxRT(), store2ConsumerRT);

                if (ThreadLocalRandom.current().nextDouble() < failRate) {
                    statsBenchmarkConsumer.getFailCount().incrementAndGet();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                } else {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            }
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "group", true, "Consumer group name, Default: benchmark_consumer");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "group prefix enable", true, "Consumer group name, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "filterType", true, "TAG, SQL92");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "expression", true, "filter expression content file path.ie: ./test/expr");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "fail rate", true, "consumer fail rate, default 0");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static void compareAndSetMax(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                break;

            prev = target.get();
        }
    }
}

class StatsBenchmarkConsumer {
    private final AtomicLong receiveMessageTotalCount = new AtomicLong(0L);

    private final AtomicLong born2ConsumerTotalRT = new AtomicLong(0L);

    private final AtomicLong store2ConsumerTotalRT = new AtomicLong(0L);

    private final AtomicLong born2ConsumerMaxRT = new AtomicLong(0L);

    private final AtomicLong store2ConsumerMaxRT = new AtomicLong(0L);

    private final AtomicLong failCount = new AtomicLong(0L);

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
            System.currentTimeMillis(),
            this.receiveMessageTotalCount.get(),
            this.born2ConsumerTotalRT.get(),
            this.store2ConsumerTotalRT.get(),
            this.failCount.get()
        };

        return snap;
    }

    public AtomicLong getReceiveMessageTotalCount() {
        return receiveMessageTotalCount;
    }

    public AtomicLong getBorn2ConsumerTotalRT() {
        return born2ConsumerTotalRT;
    }

    public AtomicLong getStore2ConsumerTotalRT() {
        return store2ConsumerTotalRT;
    }

    public AtomicLong getBorn2ConsumerMaxRT() {
        return born2ConsumerMaxRT;
    }

    public AtomicLong getStore2ConsumerMaxRT() {
        return store2ConsumerMaxRT;
    }

    public AtomicLong getFailCount() {
        return failCount;
    }
}
