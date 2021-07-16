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
package org.apache.rocketmq.tools.command.consumer;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportSubscriptionJsonCommand implements SubCommand {
    @Override
    public String commandName() {
        return "exportSubscriptionJson";
    }

    @Override
    public String commandDesc() {
        return "export subscription.json";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "choose a broker to export subscription.json");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true, "export subscription.json path | default /tmp/rocketmq/config");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();

            final String brokerAddr = commandLine.getOptionValue('b').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/config" : commandLine.getOptionValue('f')
                .trim();

            SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(
                brokerAddr, 5000);
            ConcurrentMap<String, SubscriptionGroupConfig> configMap = subscriptionGroupWrapper
                .getSubscriptionGroupTable();
            Iterator<Map.Entry<String, SubscriptionGroupConfig>> iterator = configMap.entrySet().iterator();
            while (iterator.hasNext()) {
                if (MixAll.isSysConsumerGroup(iterator.next().getKey())) {
                    iterator.remove();
                }
            }

            String subscriptionJsonString = JSON.toJSONString(subscriptionGroupWrapper, true);

            String fileName = filePath + "/subscription.json";

            MixAll.string2File(subscriptionJsonString, fileName);

            System.out.printf("export %s success", fileName);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
