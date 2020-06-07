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
package org.apache.rocketmq.example.openmessaging.consumer;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.OMSBuiltinKeys;
import io.openmessaging.api.order.ConsumeOrderContext;
import io.openmessaging.api.order.MessageOrderListener;
import io.openmessaging.api.order.OrderAction;
import io.openmessaging.api.order.OrderConsumer;
import java.util.Properties;
import org.apache.rocketmq.example.openmessaging.MqConfig;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.PropertyValueConst;


public class SimpleOrderConsumer {

    public static void main(String[] args) {
        Properties credentials = new Properties();
        credentials.setProperty(OMSBuiltinKeys.ACCESS_KEY, MqConfig.ACCESS_KEY);
        credentials.setProperty(OMSBuiltinKeys.SECRET_KEY, MqConfig.SECRET_KEY);

        MessagingAccessPoint accessPoint = OMS.builder()
            .withCredentials(credentials)
            .driver(MqConfig.DRIVER)
            .endpoint(MqConfig.ENDPOINT)
            .build();

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(PropertyKeyConst.GROUP_ID, MqConfig.GROUP_ID);

        consumerProperties.setProperty(PropertyKeyConst.MSG_TRACE_SWITCH, "false");
        consumerProperties.setProperty(PropertyKeyConst.ACL_ENABLE, "true");
        consumerProperties.setProperty(PropertyKeyConst.ACCESS_CHANNEL, PropertyValueConst.CLOUD);

        OrderConsumer consumer = accessPoint.createOrderedConsumer(consumerProperties);
        consumer.subscribe(MqConfig.ORDER_TOPIC, "*", new MessageOrderListener() {
            @Override public OrderAction consume(Message message, ConsumeOrderContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), message);
                return OrderAction.Success;
            }
        });
        consumer.start();
        System.out.printf("Consumer start success. %n");

        try {
            Thread.sleep(200000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
