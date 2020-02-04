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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

public class MQAdmin {
    private static Logger log = Logger.getLogger(MQAdmin.class);

    public static boolean createTopic(String nameSrvAddr, String clusterName, String topic,
        int queueNum) {
        int defaultWaitTime = 5;
        return createTopic(nameSrvAddr, clusterName, topic, queueNum, defaultWaitTime);
    }

    public static boolean createTopic(String nameSrvAddr, String clusterName, String topic,
        int queueNum, int waitTimeSec) {
        boolean createResult = false;
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setInstanceName(UUID.randomUUID().toString());
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        try {
            mqAdminExt.start();
            mqAdminExt.createTopic(clusterName, topic, queueNum);
        } catch (Exception e) {
        }

        long startTime = System.currentTimeMillis();
        while (!createResult) {
            createResult = checkTopicExist(mqAdminExt, topic);
            if (System.currentTimeMillis() - startTime < waitTimeSec * 1000) {
                TestUtils.waitForMoment(100);
            } else {
                log.error(String.format("timeout,but create topic[%s] failed!", topic));
                break;
            }
        }

        mqAdminExt.shutdown();
        return createResult;
    }

    private static boolean checkTopicExist(DefaultMQAdminExt mqAdminExt, String topic) {
        boolean createResult = false;
        try {
            TopicStatsTable topicInfo = mqAdminExt.examineTopicStats(topic);
            createResult = !topicInfo.getOffsetTable().isEmpty();
        } catch (Exception e) {
        }

        return createResult;
    }

    public static boolean createSub(String nameSrvAddr, String clusterName, String consumerId) {
        boolean createResult = true;
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(consumerId);
        try {
            mqAdminExt.start();
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt,
                clusterName);
            for (String addr : masterSet) {
                try {
                    mqAdminExt.createAndUpdateSubscriptionGroupConfig(addr, config);
                    log.info(String.format("create subscription group %s to %s success.\n", consumerId,
                        addr));
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000 * 1);
                }
            }
        } catch (Exception e) {
            createResult = false;
            e.printStackTrace();
        }
        mqAdminExt.shutdown();
        return createResult;
    }

    public static ClusterInfo getCluster(String nameSrvAddr) {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        ClusterInfo clusterInfo = null;
        try {
            mqAdminExt.start();
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }
        mqAdminExt.shutdown();
        return clusterInfo;
    }

    public static boolean isBrokerExist(String ns, String ip) {
        ClusterInfo clusterInfo = getCluster(ns);
        if (clusterInfo == null) {
            return false;
        } else {
            HashMap<String, BrokerData> brokers = clusterInfo.getBrokerAddrTable();
            for (String brokerName : brokers.keySet()) {
                HashMap<Long, String> brokerIps = brokers.get(brokerName).getBrokerAddrs();
                for (long brokerId : brokerIps.keySet()) {
                    if (brokerIps.get(brokerId).contains(ip))
                        return true;
                }
            }
        }

        return false;
    }

    public void getSubConnection(String nameSrvAddr, String clusterName, String consumerId) {
        boolean createResult = true;
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(consumerId);
        try {
            mqAdminExt.start();
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt,
                clusterName);
            for (String addr : masterSet) {
                try {

                    System.out.printf("create subscription group %s to %s success.\n", consumerId,
                        addr);
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000 * 1);
                }
            }
        } catch (Exception e) {
            createResult = false;
            e.printStackTrace();
        }
        mqAdminExt.shutdown();
    }

}
