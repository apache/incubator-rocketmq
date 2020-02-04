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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Set;

public class ConsumeMessageCommand implements SubCommand {

    private String topic = null;
    private long messageCount = 128;
    private DefaultMQPullConsumer defaultMQPullConsumer;


    public enum ConsumeType {
        /**
         * Topic only
         */
        DEFAULT,
        /**
         * Topic brokerName queueId set
         */
        BYQUEUE,
        /**
         * Topic brokerName queueId offset set
         */
        BYOFFSET
    }

    private static long timestampFormat(final String value) {
        long timestamp;
        try {
            timestamp = Long.parseLong(value);
        } catch (NumberFormatException e) {
            timestamp = UtilAll.parseDate(value, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS).getTime();
        }

        return timestamp;
    }
    @Override
    public String commandName() {
        return "consumeMessage";
    }

    @Override
    public String commandDesc() {
        return "Consume message";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "Topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerName", true, "Broker name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "queueId", true, "Queue Id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "Queue offset");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "Consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "beginTimestamp ", true,
                "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "endTimestamp ", true,
                "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "MessageNumber", true, "Number of message to be consumed");
        opt.setRequired(false);
        options.addOption(opt);


        return options;

    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        if (defaultMQPullConsumer == null) {
            defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, rpcHook);
        }
        defaultMQPullConsumer.setInstanceName(Long.toString(System.currentTimeMillis()));

        long offset = 0;
        long timeValueEnd = 0;
        long timeValueBegin = 0;
        String queueId = null;
        String brokerName = null;
        ConsumeType consumeType = ConsumeType.DEFAULT;

        try {
            /* Group name must be set before consumer start */
            if (commandLine.hasOption('g')) {
                String consumerGroup = commandLine.getOptionValue('g').trim();
                defaultMQPullConsumer.setConsumerGroup(consumerGroup);
            }

            defaultMQPullConsumer.start();

            topic = commandLine.getOptionValue('t').trim();

            if (commandLine.hasOption('c')) {
                messageCount = Long.parseLong(commandLine.getOptionValue('c').trim());
                if (messageCount <= 0) {
                    System.out.print("Please input a positive messageNumber!");
                    return;
                }
            }
            if (commandLine.hasOption('b')) {
                brokerName = commandLine.getOptionValue('b').trim();

            }
            if (commandLine.hasOption('i')) {
                if (!commandLine.hasOption('b')) {
                    System.out.print("Please set the brokerName before queueId!");
                    return;
                }
                queueId = commandLine.getOptionValue('i').trim();

                consumeType = ConsumeType.BYQUEUE;
            }
            if (commandLine.hasOption('o')) {
                if (consumeType != ConsumeType.BYQUEUE) {
                    System.out.print("Please set queueId before offset!");
                    return;
                }
                offset = Long.parseLong(commandLine.getOptionValue('o').trim());
                consumeType = ConsumeType.BYOFFSET;
            }

            long now = System.currentTimeMillis();
            if (commandLine.hasOption('s')) {
                String timestampStr = commandLine.getOptionValue('s').trim();
                timeValueBegin = timestampFormat(timestampStr);
                if (timeValueBegin > now) {
                    System.out.print("Please set the beginTimestamp before now!");
                    return;
                }
            }
            if (commandLine.hasOption('e')) {
                String timestampStr = commandLine.getOptionValue('e').trim();
                timeValueEnd = timestampFormat(timestampStr);
                if (timeValueEnd > now) {
                    System.out.print("Please set the endTimestamp before now!");
                    return;
                }
                if (timeValueBegin > timeValueEnd) {
                    System.out.print("Please make sure that the beginTimestamp is less than or equal to the endTimestamp");
                    return;
                }
            }

            switch (consumeType) {
                case DEFAULT:
                    executeDefault(timeValueBegin, timeValueEnd);
                    break;
                case BYOFFSET:
                    executeByCondition(brokerName, queueId, offset, timeValueBegin, timeValueEnd);
                    break;
                case BYQUEUE:
                    executeByCondition(brokerName, queueId, 0, timeValueBegin, timeValueEnd);
                    break;
                default:
                    System.out.print("Unknown type of consume!");
                    break;
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQPullConsumer.shutdown();
        }
    }

    private void pullMessageByQueue(MessageQueue mq, long minOffset, long maxOffset) {
        READQ:
        for (long offset = minOffset; offset <= maxOffset; ) {
            PullResult pullResult = null;
            try {
                pullResult = defaultMQPullConsumer.pull(mq, "*", offset, (int)(maxOffset - offset + 1));
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            if (pullResult != null) {
                offset = pullResult.getNextBeginOffset();
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        System.out.print("Consume ok\n");
                        PrintMessageByQueueCommand.printMessage(pullResult.getMsgFoundList(), "UTF-8",
                            true, true);
                        break;
                    case NO_MATCHED_MSG:
                        System.out.printf("%s no matched msg. status=%s, offset=%s\n", mq, pullResult.getPullStatus(),
                            offset);
                        break;
                    case NO_NEW_MSG:
                    case OFFSET_ILLEGAL:
                        System.out.printf("%s print msg finished. status=%s, offset=%s\n", mq,
                            pullResult.getPullStatus(), offset);
                        break READQ;
                    default:
                        break;
                }
            }
        }
    }

    private void executeDefault(long timeValueBegin, long timeValueEnd) {
        try {
            Set<MessageQueue> mqs = defaultMQPullConsumer.fetchSubscribeMessageQueues(topic);
            long countLeft = messageCount;
            for (MessageQueue mq : mqs) {
                if (countLeft == 0) {
                    return;
                }
                long minOffset = defaultMQPullConsumer.minOffset(mq);
                long maxOffset = defaultMQPullConsumer.maxOffset(mq);
                if (timeValueBegin > 0) {
                    minOffset = defaultMQPullConsumer.searchOffset(mq, timeValueBegin);
                }
                if (timeValueEnd > 0) {
                    maxOffset = defaultMQPullConsumer.searchOffset(mq, timeValueEnd);
                }
                if (maxOffset - minOffset > countLeft) {
                    System.out.printf("The older %d message of the %d queue will be provided\n", countLeft, mq.getQueueId());
                    maxOffset = minOffset + countLeft - 1;
                    countLeft = 0;
                } else {
                    countLeft = countLeft - (maxOffset - minOffset) - 1;
                }

                pullMessageByQueue(mq, minOffset, maxOffset);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeByCondition(String brokerName, String queueId, long offset, long timeValueBegin, long timeValueEnd) {
        MessageQueue mq = new MessageQueue(topic, brokerName, Integer.parseInt(queueId));
        try {
            long minOffset = defaultMQPullConsumer.minOffset(mq);
            long maxOffset = defaultMQPullConsumer.maxOffset(mq);
            if (timeValueBegin > 0) {
                minOffset = defaultMQPullConsumer.searchOffset(mq, timeValueBegin);
            }
            if (timeValueEnd > 0) {
                maxOffset = defaultMQPullConsumer.searchOffset(mq, timeValueEnd);
            }
            if (offset > maxOffset) {
                System.out.printf("%s no matched msg, offset=%s\n", mq, offset);
                return;
            }
            minOffset = minOffset > offset ? minOffset : offset;
            if (maxOffset - minOffset > messageCount) {
                System.out.printf("The oldler %d message will be provided\n", messageCount);
                maxOffset = minOffset + messageCount - 1;
            }

            pullMessageByQueue(mq, minOffset, maxOffset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}