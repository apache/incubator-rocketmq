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
package org.apache.rocketmq.tools.command.message;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class PrintMessageSubCommand implements SubCommand {

    public static long timestampFormat(final String value) {
        long timestamp = 0;
        try {
            timestamp = Long.parseLong(value);
        } catch (NumberFormatException e) {
            timestamp = UtilAll.parseDate(value, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS).getTime();
        }

        return timestamp;
    }

    public static void printMessage(final List<MessageExt> msgs, final String charsetName, boolean printBody) {
        for (MessageExt msg : msgs) {
            try {
                System.out.printf("MSGID: %s %s BODY: %s%n", msg.getMsgId(), msg.toString(),
                    printBody ? new String(msg.getBody(), charsetName) : "NOT PRINT BODY");
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    @Override
    public String commandName() {
        return "printMsg";
    }

    @Override
    public String commandDesc() {
        return "Print Message Detail";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "charsetName ", true, "CharsetName(eg: UTF-8,GBK)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "subExpression ", true, "Subscribe Expression(eg: TagA || TagB)");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
            new Option("b", "beginTimestamp ", true,
                "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
            new Option("e", "endTimestamp ", true,
                "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
            new Option("d", "printBody ", true,
                "print body");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "Queue Offset");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, rpcHook);

        try {
            int maxNums = 32;

            String topic = commandLine.getOptionValue('t').trim();

            String charsetName =
                !commandLine.hasOption('c') ? "UTF-8" : commandLine.getOptionValue('c').trim();

            String subExpression =
                !commandLine.hasOption('s') ? "*" : commandLine.getOptionValue('s').trim();

            boolean printBody = !commandLine.hasOption('d') || Boolean.parseBoolean(commandLine.getOptionValue('d').trim());

            consumer.start();

            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            for (MessageQueue mq : mqs) {
                long minOffset = consumer.minOffset(mq);
                long maxOffset = consumer.maxOffset(mq);

                if (commandLine.hasOption('b')) {
                    String timestampStr = commandLine.getOptionValue('b').trim();
                    long timeValue = timestampFormat(timestampStr);
                    minOffset = consumer.searchOffset(mq, timeValue);
                }

                if (commandLine.hasOption('e')) {
                    String timestampStr = commandLine.getOptionValue('e').trim();
                    long timeValue = timestampFormat(timestampStr);
                    maxOffset = consumer.searchOffset(mq, timeValue);
                }

                if (commandLine.hasOption("o")) {
                    long offset = Long.parseLong(commandLine.getOptionValue('o').trim());
                    if (offset < minOffset || offset > maxOffset) {
                        System.out.printf("Illegal consumeQueue offset,current consumeQueue: %s, maxOffset: %s, minOffset: %s",
                            mq.getQueueId(), maxOffset, minOffset);
                        return;
                    }
                    minOffset = offset;
                    maxOffset = offset + 1;
                    maxNums = 1;
                }

                System.out.printf("minOffset=%s, maxOffset=%s, %s", minOffset, maxOffset, mq);

                READQ:
                for (long offset = minOffset; offset < maxOffset; ) {
                    try {
                        PullResult pullResult = consumer.pull(mq, subExpression, offset, maxNums);
                        offset = pullResult.getNextBeginOffset();
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                printMessage(pullResult.getMsgFoundList(), charsetName, printBody);
                                break;
                            case NO_MATCHED_MSG:
                                System.out.printf("%s no matched msg. status=%s, offset=%s", mq, pullResult.getPullStatus(), offset);
                                break;
                            case NO_NEW_MSG:
                            case OFFSET_ILLEGAL:
                                System.out.printf("%s print msg finished. status=%s, offset=%s", mq, pullResult.getPullStatus(), offset);
                                break READQ;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            consumer.shutdown();
        }
    }
}
