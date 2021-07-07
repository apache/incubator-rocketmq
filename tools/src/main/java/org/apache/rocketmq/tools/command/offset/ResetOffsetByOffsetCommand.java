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

package org.apache.rocketmq.tools.command.offset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.AbstractSubCommand;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ResetOffsetByOffsetCommand extends AbstractSubCommand {

    @Override
    public String commandName() {
        return "resetOffsetByOffset";
    }

    @Override
    public String commandDesc() {
        return "Reset consumer offset by offset(without client restart).";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerName", true, "broker name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "queueId", true, "queue Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "set the offset");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "cplus", false, "reset c++ client offset");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = createMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String topic = commandLine.getOptionValue("t").trim();
            String brokerName = commandLine.getOptionValue("b").trim();
            int queueId = Integer.parseInt(commandLine.getOptionValue("i").trim());
            String group = commandLine.getOptionValue("g").trim();
            long offset = Long.parseLong(commandLine.getOptionValue("o").trim());
            boolean isC = false;
            if (commandLine.hasOption('c')) {
                isC = true;
            }

            defaultMQAdminExt.start();

            boolean result = defaultMQAdminExt.resetOffsetByOffset(topic, group, brokerName, queueId, offset, isC);

            System.out.printf("rollback consumer offset by specified brokerName[%s] group[%s], topic[%s], queue[%s], offset[%s], result[%s]%n",
                    brokerName, group, topic, queueId, offset, result);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
