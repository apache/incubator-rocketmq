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

package org.apache.rocketmq.test.base;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.test.client.rmq.RMQAsyncSendProducer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQTransactionalProducer;
import org.apache.rocketmq.test.clientinterface.AbstractMQConsumer;
import org.apache.rocketmq.test.clientinterface.AbstractMQProducer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.AbstractListener;
import org.apache.rocketmq.test.util.MQAdmin;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;

import static org.awaitility.Awaitility.await;

public class BaseConf {
    public final static String nsAddr;
    protected final static String broker1Name;
    protected final static String broker2Name;
    protected final static String clusterName;
    protected final static int brokerNum;
    protected final static int waitTime = 5;
    protected final static int consumeTime = 2 * 60 * 1000;
    protected final static int QUEUE_NUMBERS = 8;
    protected final static NamesrvController namesrvController;
    protected final static BrokerController brokerController1;
    protected final static BrokerController brokerController2;
    protected final static List<BrokerController> brokerControllerList;
    protected final static Map<String, BrokerController> brokerControllerMap;
    protected final static List<Object> mqClients = new ArrayList<Object>();
    protected final static boolean debug = false;
    private final static Logger log = Logger.getLogger(BaseConf.class);

    static {
    	System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        namesrvController = IntegrationTestBase.createAndStartNamesrv();
        nsAddr = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();
        brokerController1 = IntegrationTestBase.createAndStartBroker(nsAddr);
        brokerController2 = IntegrationTestBase.createAndStartBroker(nsAddr);
        clusterName = brokerController1.getBrokerConfig().getBrokerClusterName();
        broker1Name = brokerController1.getBrokerConfig().getBrokerName();
        broker2Name = brokerController2.getBrokerConfig().getBrokerName();
        brokerNum = 2;
        brokerControllerList = ImmutableList.of(brokerController1, brokerController2);
        brokerControllerMap = brokerControllerList.stream().collect(Collectors.toMap(input -> input.getBrokerConfig().getBrokerName(), Function.identity()));
    }

    public BaseConf() {

    }

    // This method can't be placed in the static block of BaseConf, which seems to lead to a strange dead lock.
    public static void waitBrokerRegistered(final String nsAddr, final String clusterName) {
        final DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(500);
        mqAdminExt.setNamesrvAddr(nsAddr);
        try {
            mqAdminExt.start();
            await().atMost(30, TimeUnit.SECONDS).until(() -> {
                List<BrokerData> brokerDatas = mqAdminExt.examineTopicRouteInfo(clusterName).getBrokerDatas();
                return brokerDatas.size() == brokerNum;
            });
        } catch (MQClientException e) {
            log.error("init failed, please check BaseConf");
        }
        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
    }

    public static String initTopic() {
        String topic = "tt-" + MQRandomUtils.getRandomTopic();
        IntegrationTestBase.initTopic(topic, nsAddr, clusterName);

        return topic;
    }

    public static String initConsumerGroup() {
        String group = MQRandomUtils.getRandomConsumerGroup();
        return initConsumerGroup(group);
    }

    public static String initConsumerGroup(String group) {
        MQAdmin.createSub(nsAddr, clusterName, group);
        return group;
    }

    public static RMQNormalProducer getProducer(String nsAddr, String topic) {
        return getProducer(nsAddr, topic, false);
    }

    public static RMQNormalProducer getProducer(String nsAddr, String topic, boolean useTLS) {
        RMQNormalProducer producer = new RMQNormalProducer(nsAddr, topic, useTLS);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQTransactionalProducer getTransactionalProducer(String nsAddr, String topic, TransactionListener transactionListener) {
        RMQTransactionalProducer producer = new RMQTransactionalProducer(nsAddr, topic, false, transactionListener);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQNormalProducer getProducer(String nsAddr, String topic, String producerGoup,
        String instanceName) {
        RMQNormalProducer producer = new RMQNormalProducer(nsAddr, topic, producerGoup,
            instanceName);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQAsyncSendProducer getAsyncProducer(String nsAddr, String topic) {
        RMQAsyncSendProducer producer = new RMQAsyncSendProducer(nsAddr, topic);
        if (debug) {
            producer.setDebug();
        }
        mqClients.add(producer);
        return producer;
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String topic, String subExpression,
        AbstractListener listener) {
        return getConsumer(nsAddr, topic, subExpression, listener, false);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String topic, String subExpression,
        AbstractListener listener, boolean useTLS) {
        String consumerGroup = initConsumerGroup();
        return getConsumer(nsAddr, consumerGroup, topic, subExpression, listener, useTLS);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String consumerGroup, String topic,
        String subExpression, AbstractListener listener) {
        return getConsumer(nsAddr, consumerGroup, topic, subExpression, listener, false);
    }

    public static RMQNormalConsumer getConsumer(String nsAddr, String consumerGroup, String topic,
        String subExpression, AbstractListener listener, boolean useTLS) {
        RMQNormalConsumer consumer = ConsumerFactory.getRMQNormalConsumer(nsAddr, consumerGroup,
            topic, subExpression, listener, useTLS);
        if (debug) {
            consumer.setDebug();
        }
        mqClients.add(consumer);
        log.info(String.format("consumer[%s] start,topic[%s],subExpression[%s]", consumerGroup,
            topic, subExpression));
        return consumer;
    }

    public static void shutdown() {
        ImmutableList<Object> mqClients = ImmutableList.copyOf(BaseConf.mqClients);
        BaseConf.mqClients.clear();
        shutdown(mqClients);
    }

    public static void shutdown(List<Object> mqClients) {
        mqClients.forEach(mqClient -> ForkJoinPool.commonPool().execute(() -> {
            if (mqClient instanceof AbstractMQProducer) {
                ((AbstractMQProducer) mqClient).shutdown();
            } else if (mqClient instanceof AbstractMQConsumer) {
                ((AbstractMQConsumer) mqClient).shutdown();
            } else if (mqClient instanceof MQAdminExt) {
                ((MQAdminExt) mqClient).shutdown();
            } else if (mqClient instanceof MQProducer) {
                ((MQProducer) mqClient).shutdown();
            } else if (mqClient instanceof MQPullConsumer) {
                ((MQPullConsumer) mqClient).shutdown();
            } else if (mqClient instanceof MQPushConsumer) {
                ((MQPushConsumer) mqClient).shutdown();
            }
        }));
    }
}
