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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.srvutil.ServerUtil;

public class TransactionProducer {
    private static final long START_TIME = System.currentTimeMillis();
    private static final AtomicLong MSG_COUNT = new AtomicLong(0);

    //broker max check times should less than this value
    static final int MAX_CHECK_RESULT_IN_MSG = 20;

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("TransactionProducer", args, buildCommandlineOptions(options), new PosixParser());
        TxSendConfig config = new TxSendConfig();
        config.topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        config.threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 32;
        config.messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 2048;
        config.sendRollbackRate = commandLine.hasOption("sr") ? Double.parseDouble(commandLine.getOptionValue("sr")) : 0.0;
        config.sendUnknownRate = commandLine.hasOption("su") ? Double.parseDouble(commandLine.getOptionValue("su")) : 0.0;
        config.checkRollbackRate = commandLine.hasOption("cr") ? Double.parseDouble(commandLine.getOptionValue("cr")) : 0.0;
        config.checkUnknownRate = commandLine.hasOption("cu") ? Double.parseDouble(commandLine.getOptionValue("cu")) : 0.0;
        config.batchId = commandLine.hasOption("b") ? Long.parseLong(commandLine.getOptionValue("b")) : System.currentTimeMillis();
        config.sendInterval = commandLine.hasOption("i") ? Integer.parseInt(commandLine.getOptionValue("i")) : 0;

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(config.threadCount);

        final StatsBenchmarkTProducer statsBenchmark = new StatsBenchmarkTProducer();

        final Timer timer = new Timer("BenchmarkTimerThread", true);

        final LinkedList<Snapshot> snapshotList = new LinkedList<>();

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                while (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Snapshot begin = snapshotList.getFirst();
                    Snapshot end = snapshotList.getLast();

                    final long sendCount = (end.sendRequestSuccessCount - begin.sendRequestSuccessCount)
                            + (end.sendRequestFailedCount - begin.sendRequestFailedCount);
                    final long sendTps = (sendCount * 1000L) / (end.endTime - begin.endTime);
                    final double averageRT = (end.sendMessageTimeTotal - begin.sendMessageTimeTotal) / (double) (end.sendRequestSuccessCount - begin.sendRequestSuccessCount);

                    final long failCount = end.sendRequestFailedCount - begin.sendRequestFailedCount;
                    final long checkCount = end.checkCount - begin.checkCount;
                    final long unexpectedCheck = end.unexpectedCheckCount - begin.unexpectedCheckCount;
                    final long dupCheck = end.duplicatedCheck - begin.duplicatedCheck;

                    System.out.printf(
                        "Send TPS:%5d Max RT:%5d AVG RT:%3.1f Send Failed: %d check: %d unexpectedCheck: %d duplicatedCheck: %d %n",
                            sendTps, statsBenchmark.getSendMessageMaxRT().get(), averageRT, failCount, checkCount,
                            unexpectedCheck, dupCheck);
                    statsBenchmark.getSendMessageMaxRT().set(0);
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

        final TransactionListener transactionCheckListener = new TransactionListenerImpl(statsBenchmark, config);
        final TransactionMQProducer producer = new TransactionMQProducer("benchmark_transaction_producer");
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));
        producer.setTransactionListener(transactionCheckListener);
        producer.setDefaultTopicQueueNums(1000);
        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            producer.setNamesrvAddr(ns);
        }
        producer.start();

        for (int i = 0; i < config.threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        boolean success = false;
                        final long beginTimestamp = System.currentTimeMillis();
                        try {
                            SendResult sendResult =
                                    producer.sendMessageInTransaction(buildMessage(config), null);
                            success = sendResult != null && sendResult.getSendStatus() == SendStatus.SEND_OK;
                        } catch (Throwable e) {
                            success = false;
                        } finally {
                            final long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageTimeTotal().addAndGet(currentRT);
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            while (currentRT > prevMaxRT) {
                                boolean updated = statsBenchmark.getSendMessageMaxRT()
                                        .compareAndSet(prevMaxRT, currentRT);
                                if (updated)
                                    break;

                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                            if (success) {
                                statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                            } else {
                                statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            }
                            if (config.sendInterval > 0) {
                                try {
                                    Thread.sleep(config.sendInterval);
                                } catch (InterruptedException e) {
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    private static Message buildMessage(TxSendConfig config) {
        byte[] bs = new byte[config.messageSize];
        ThreadLocalRandom r = ThreadLocalRandom.current();
        r.nextBytes(bs);

        ByteBuffer buf = ByteBuffer.wrap(bs);
        buf.putLong(config.batchId);
        long sendMachineId = START_TIME << 32;
        long msgId = sendMachineId | MSG_COUNT.getAndIncrement();
        buf.putLong(msgId);

        // save send tx result in message
        if (r.nextDouble() < config.sendRollbackRate) {
            buf.put((byte) LocalTransactionState.ROLLBACK_MESSAGE.ordinal());
        } else if (r.nextDouble() < config.sendUnknownRate) {
            buf.put((byte) LocalTransactionState.UNKNOW.ordinal());
        } else {
            buf.put((byte) LocalTransactionState.COMMIT_MESSAGE.ordinal());
        }

        // save check tx result in message
        for (int i = 0; i < MAX_CHECK_RESULT_IN_MSG; i++) {
            if (r.nextDouble() < config.checkRollbackRate) {
                buf.put((byte) LocalTransactionState.ROLLBACK_MESSAGE.ordinal());
            } else if (r.nextDouble() < config.checkUnknownRate) {
                buf.put((byte) LocalTransactionState.UNKNOW.ordinal());
            } else {
                buf.put((byte) LocalTransactionState.COMMIT_MESSAGE.ordinal());
            }
        }

        Message msg = new Message();
        msg.setTopic(config.topic);

        msg.setBody(bs);
        return msg;
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("w", "threadCount", true, "Thread count, Default: 32");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 2048");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sr", "send rollback rate", true, "Send rollback rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("su", "send unknown rate", true, "Send unknown rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("cr", "check rollback rate", true, "Check rollback rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("cu", "check unknown rate", true, "Check unknown rate, Default: 0.0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "test batch id", true, "test batch id, Default: System.currentMillis()");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "send interval", true, "sleep interval in millis between messages, Default: 0");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}

class TransactionListenerImpl implements TransactionListener {
    private StatsBenchmarkTProducer statBenchmark;
    private TxSendConfig sendConfig;
    private final LRUMap<Long, Integer> cache = new LRUMap<>(200000);

    private class MsgMeta {
        long batchId;
        long msgId;
        LocalTransactionState sendResult;
        List<LocalTransactionState> checkResult;
    }

    public TransactionListenerImpl(StatsBenchmarkTProducer statsBenchmark, TxSendConfig sendConfig) {
        this.statBenchmark = statsBenchmark;
        this.sendConfig = sendConfig;
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        return parseFromMsg(msg).sendResult;
    }

    private MsgMeta parseFromMsg(Message msg) {
        byte[] bs = msg.getBody();
        ByteBuffer buf = ByteBuffer.wrap(bs);
        MsgMeta msgMeta = new MsgMeta();
        msgMeta.batchId = buf.getLong();
        msgMeta.msgId = buf.getLong();
        msgMeta.sendResult = LocalTransactionState.values()[buf.get()];
        msgMeta.checkResult = new ArrayList<>();
        for (int i = 0; i < TransactionProducer.MAX_CHECK_RESULT_IN_MSG; i++) {
            msgMeta.checkResult.add(LocalTransactionState.values()[buf.get()]);
        }
        return msgMeta;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        MsgMeta msgMeta = parseFromMsg(msg);
        if (msgMeta.batchId != sendConfig.batchId) {
            // message not generated in this test
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        statBenchmark.getCheckCount().incrementAndGet();

        int times = 0;
        try {
            String checkTimes = msg.getUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
            times = Integer.parseInt(checkTimes);
        } catch (Exception e) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        times = times <= 0 ? 1 : times;

        boolean dup;
        synchronized (cache) {
            Integer oldCheckLog = cache.get(msgMeta.msgId);
            Integer newCheckLog;
            if (oldCheckLog == null) {
                newCheckLog = 1 << (times - 1);
            } else {
                newCheckLog = oldCheckLog | (1 << (times - 1));
            }
            dup = newCheckLog.equals(oldCheckLog);
        }
        if (dup) {
            statBenchmark.getDuplicatedCheckCount().incrementAndGet();
        }
        if (msgMeta.sendResult != LocalTransactionState.UNKNOW) {
            System.out.printf("%s unexpected check: msgId=%s,txId=%s,checkTimes=%s,sendResult=%s\n",
                    new SimpleDateFormat("HH:mm:ss,SSS").format(new Date()),
                    msg.getMsgId(), msg.getTransactionId(),
                    msg.getUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES),
                    msgMeta.sendResult.toString());
            statBenchmark.getUnexpectedCheckCount().incrementAndGet();
            return msgMeta.sendResult;
        }

        for (int i = 0; i < times - 1; i++) {
            LocalTransactionState s = msgMeta.checkResult.get(i);
            if (s != LocalTransactionState.UNKNOW) {
                System.out.printf("%s unexpected check: msgId=%s,txId=%s,checkTimes=%s,sendResult,lastCheckResult=%s\n",
                        new SimpleDateFormat("HH:mm:ss,SSS").format(new Date()),
                        msg.getMsgId(), msg.getTransactionId(),
                        msg.getUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES), s);
                statBenchmark.getUnexpectedCheckCount().incrementAndGet();
                return s;
            }
        }
        return msgMeta.checkResult.get(times - 1);
    }
}

class Snapshot {
    long endTime;

    long sendRequestSuccessCount;

    long sendRequestFailedCount;

    long sendMessageTimeTotal;

    long sendMessageMaxRT;

    long checkCount;

    long unexpectedCheckCount;

    long duplicatedCheck;
}

class StatsBenchmarkTProducer {
    private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

    private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

    private final AtomicLong sendMessageTimeTotal = new AtomicLong(0L);

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    private final AtomicLong checkCount = new AtomicLong(0L);

    private final AtomicLong unexpectedCheckCount = new AtomicLong(0L);

    private final AtomicLong duplicatedCheckCount = new AtomicLong(0);

    public Snapshot createSnapshot() {
        Snapshot s = new Snapshot();
        s.endTime = System.currentTimeMillis();
        s.sendRequestSuccessCount = sendRequestSuccessCount.get();
        s.sendRequestFailedCount = sendRequestFailedCount.get();
        s.sendMessageTimeTotal = sendMessageTimeTotal.get();
        s.sendMessageMaxRT = sendMessageMaxRT.get();
        s.checkCount = checkCount.get();
        s.unexpectedCheckCount = unexpectedCheckCount.get();
        s.duplicatedCheck = duplicatedCheckCount.get();
        return s;
    }

    public AtomicLong getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }

    public AtomicLong getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }

    public AtomicLong getSendMessageTimeTotal() {
        return sendMessageTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }

    public AtomicLong getCheckCount() {
        return checkCount;
    }

    public AtomicLong getUnexpectedCheckCount() {
        return unexpectedCheckCount;
    }

    public AtomicLong getDuplicatedCheckCount() {
        return duplicatedCheckCount;
    }
}

class TxSendConfig {
    String topic;
    int threadCount;
    int messageSize;
    double sendRollbackRate;
    double sendUnknownRate;
    double checkRollbackRate;
    double checkUnknownRate;
    long batchId;
    int sendInterval;
}

class LRUMap<K, V> extends LinkedHashMap<K, V> {

    private int maxSize;

    public LRUMap(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > maxSize;
    }
}
