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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class QueryMsgByUniqueKeySubCommand implements SubCommand {

    private DefaultMQAdminExt defaultMQAdminExt;

    private DefaultMQAdminExt createMQAdminExt(RPCHook rpcHook) throws SubCommandException {
        if (this.defaultMQAdminExt != null) {
            return defaultMQAdminExt;
        } else {
            defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
            defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                defaultMQAdminExt.start();
            }
            catch (Exception e) {
                throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
            }
            return defaultMQAdminExt;
        }
    }

    public static void queryById(final DefaultMQAdminExt admin, final String topic,
        final String msgId) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException, IOException {
        MessageExt msg = admin.viewMessage(topic, msgId);

        String bodyTmpFilePath = createBodyFile(msg);

        System.out.printf("%-20s %s%n",
            "Topic:",
            msg.getTopic()
        );

        System.out.printf("%-20s %s%n",
            "Tags:",
            "[" + msg.getTags() + "]"
        );

        System.out.printf("%-20s %s%n",
            "Keys:",
            "[" + msg.getKeys() + "]"
        );

        System.out.printf("%-20s %d%n",
            "Queue ID:",
            msg.getQueueId()
        );

        System.out.printf("%-20s %d%n",
            "Queue Offset:",
            msg.getQueueOffset()
        );

        System.out.printf("%-20s %d%n",
            "CommitLog Offset:",
            msg.getCommitLogOffset()
        );

        System.out.printf("%-20s %d%n",
            "Reconsume Times:",
            msg.getReconsumeTimes()
        );

        System.out.printf("%-20s %s%n",
            "Born Timestamp:",
            UtilAll.timeMillisToHumanString2(msg.getBornTimestamp())
        );

        System.out.printf("%-20s %s%n",
            "Store Timestamp:",
            UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp())
        );

        System.out.printf("%-20s %s%n",
            "Born Host:",
            RemotingHelper.parseSocketAddressAddr(msg.getBornHost())
        );

        System.out.printf("%-20s %s%n",
            "Store Host:",
            RemotingHelper.parseSocketAddressAddr(msg.getStoreHost())
        );

        System.out.printf("%-20s %d%n",
            "System Flag:",
            msg.getSysFlag()
        );

        System.out.printf("%-20s %s%n",
            "Properties:",
            msg.getProperties() != null ? msg.getProperties().toString() : ""
        );

        System.out.printf("%-20s %s%n",
            "Message Body Path:",
            bodyTmpFilePath
        );

        try {
            List<MessageTrack> mtdList = admin.messageTrackDetail(msg);
            if (mtdList.isEmpty()) {
                System.out.printf("%n%nWARN: No Consumer");
            } else {
                System.out.printf("%n%n");
                for (MessageTrack mt : mtdList) {
                    System.out.printf("%s", mt);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String createBodyFile(MessageExt msg) throws IOException {
        DataOutputStream dos = null;
        try {
            String bodyTmpFilePath = "/tmp/rocketmq/msgbodys";
            File file = new File(bodyTmpFilePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            bodyTmpFilePath = bodyTmpFilePath + "/" + msg.getMsgId();
            dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath));
            dos.write(msg.getBody());
            return bodyTmpFilePath;
        } finally {
            if (dos != null)
                dos.close();
        }
    }

    @Override
    public String commandName() {
        return "queryMsgByUniqueKey";
    }

    @Override
    public String commandDesc() {
        return "Query Message by Unique key";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "clientId", true, "The consumer's client id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "The topic of msg");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        try {

            defaultMQAdminExt =  createMQAdminExt(rpcHook);

            final String msgId = commandLine.getOptionValue('i').trim();
            final String topic = commandLine.getOptionValue('t').trim();
            if (commandLine.hasOption('g') && commandLine.hasOption('d')) {
                final String consumerGroup = commandLine.getOptionValue('g').trim();
                final String clientId = commandLine.getOptionValue('d').trim();
                ConsumeMessageDirectlyResult result =
                    defaultMQAdminExt.consumeMessageDirectly(consumerGroup, clientId, topic, msgId);
                System.out.printf("%s", result);
            } else {
                queryById(defaultMQAdminExt, topic, msgId);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
