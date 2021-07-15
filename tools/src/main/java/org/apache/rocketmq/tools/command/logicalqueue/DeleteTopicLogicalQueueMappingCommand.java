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
package org.apache.rocketmq.tools.command.logicalqueue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTopicLogicalQueueMappingCommand implements SubCommand {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STDOUT_LOGGER_NAME);

    @Override public String commandName() {
        return "deleteTopicLogicalQueueMapping";
    }

    @Override public String commandDesc() {
        return "delete logical queue mapping info of a topic";
    }

    @Override public Options buildCommandlineOptions(Options options) {
        Option opt;

        opt = new Option("t", "topic", true, "topic name.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "cluster name.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "broker addr.");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String topic = commandLine.getOptionValue("t").trim();
            List<String> brokerAddrs;
            if (commandLine.hasOption("b")) {
                brokerAddrs = Collections.singletonList(commandLine.getOptionValue("c").trim());
            } else if (commandLine.hasOption("c")) {
                String clusterName = commandLine.getOptionValue("c").trim();
                brokerAddrs = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName).stream().sorted().collect(Collectors.toList());
            } else {
                ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
                return;
            }
            for (String brokerAddr : brokerAddrs) {
                log.info("deleteTopicLogicalQueueMapping {} {}", brokerAddr, topic);
                defaultMQAdminExt.deleteTopicLogicalQueueMapping(brokerAddr, topic);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
