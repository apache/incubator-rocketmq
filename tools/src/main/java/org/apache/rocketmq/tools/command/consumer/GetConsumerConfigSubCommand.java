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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetConsumerConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getConsumerConfig";
    }

    @Override
    public String commandDesc() {
        return "Get consumer config by subscription group name!";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("g", "groupName", true, "subscription group name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        String groupName = commandLine.getOptionValue('g').trim();
        try {
            adminExt.start();
            List<ConsumerConfigInfo> consumerConfigInfoList = new ArrayList<>();
            ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            for (String brokerName : clusterInfo.getBrokerAddrTable().keySet()) {
                String clusterName = this.getClusterName(brokerName, clusterAddrTable);
                String brokerAddress = clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr();
                SubscriptionGroupConfig subscriptionGroupConfig = adminExt.examineSubscriptionGroupConfig(brokerAddress, groupName);
                if (subscriptionGroupConfig == null) {
                    continue;
                }
                consumerConfigInfoList.add(new ConsumerConfigInfo(clusterName, brokerName, subscriptionGroupConfig));
            }
            if (CollectionUtils.isEmpty(consumerConfigInfoList)) {
                return;
            }
            for (ConsumerConfigInfo info : consumerConfigInfoList) {
                System.out.printf("=============================%s:%s=============================\n",
                    info.getClusterName(), info.getBrokerName());
                SubscriptionGroupConfig config = info.getSubscriptionGroupConfig();
                Field[] fields = config.getClass().getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    if (field.get(config) != null) {
                        System.out.printf("%s%-40s=  %s\n", "", field.getName(), field.get(config).toString());
                    } else {
                        System.out.printf("%s%-40s=  %s\n", "", field.getName(), "");
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }

    private String getClusterName(String brokeName, Map<String, Set<String>> clusterAddrTable) {
        for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
            Set<String> brokerNameSet = entry.getValue();
            if (brokerNameSet.contains(brokeName)) {
                return entry.getKey();
            }
        }
        return null;
    }
}

class ConsumerConfigInfo {
    private String clusterName;

    private String brokerName;

    private SubscriptionGroupConfig subscriptionGroupConfig;

    public ConsumerConfigInfo(String clusterName, String brokerName, SubscriptionGroupConfig subscriptionGroupConfig) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.subscriptionGroupConfig = subscriptionGroupConfig;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerNameList) {
        this.brokerName = brokerName;
    }

    public SubscriptionGroupConfig getSubscriptionGroupConfig() {
        return subscriptionGroupConfig;
    }

    public void setSubscriptionGroupConfig(SubscriptionGroupConfig subscriptionGroupConfig) {
        this.subscriptionGroupConfig = subscriptionGroupConfig;
    }
}
