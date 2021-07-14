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
package org.apache.rocketmq.tools.command.connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ProducerConnectionMap;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ProducerConnectionAllSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "producerConnectionAll";
    }

    @Override
    public String commandDesc() {
        return "Query all producer's socket connection and client version";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "Query productions to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "Query productions to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);



        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            ProducerConnectionMap producerConnectionMap = null;

            if (commandLine.hasOption('b')) {
                String brokeraddr = commandLine.getOptionValue('b').trim();
                producerConnectionMap = defaultMQAdminExt.examineAllProducerConnectionInfo(brokeraddr);
            } else if(commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                String firstBrokerAddr = masterSet.iterator().next();
                producerConnectionMap = defaultMQAdminExt.examineAllProducerConnectionInfo(firstBrokerAddr);
            }

            Map<String, HashSet<Connection>> connectionMap = producerConnectionMap.getConnectionMap();

            Iterator<Map.Entry<String, HashSet<Connection>>> it = connectionMap.entrySet().iterator();

            int i = 1;
            while (it.hasNext()) {
                Map.Entry<String, HashSet<Connection>> entry = it.next();
                String producerGroup = entry.getKey();
                HashSet<Connection> connctionSet = entry.getValue();

                for (Connection conn : connctionSet) {
                    System.out.printf("%04d %-32s %-32s %-22s %-8s %s%n",
                            i++,
                            producerGroup,
                            conn.getClientId(),
                            conn.getClientAddr(),
                            conn.getLanguage(),
                            MQVersion.getVersionDesc(conn.getVersion())
                    );
                }
            }


        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
