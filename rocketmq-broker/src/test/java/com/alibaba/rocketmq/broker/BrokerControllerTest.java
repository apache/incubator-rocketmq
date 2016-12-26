/**
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

package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shtykh_roman
 */
public class BrokerControllerTest {
    protected Logger logger = LoggerFactory.getLogger(BrokerControllerTest.class);

    private static final int RESTART_NUM = 3;

    /**
     * Tests if the controller can be properly stopped and started.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testRestart() throws Exception {

        for (int i = 0; i < RESTART_NUM; i++) {
            BrokerController brokerController = new BrokerController(//
                new BrokerConfig(), //
                new NettyServerConfig(), //
                new NettyClientConfig(), //
                new MessageStoreConfig());
            boolean initResult = brokerController.initialize();
            Assert.assertTrue(initResult);
            logger.info("Broker is initialized " + initResult);
            brokerController.start();
            logger.info("Broker is started");

            brokerController.shutdown();
            logger.info("Broker is stopped");
        }
    }
}
