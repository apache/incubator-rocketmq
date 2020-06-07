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
package org.apache.rocketmq.example.openmessaging.producer;

import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.OMS;
import io.openmessaging.api.OMSBuiltinKeys;
import io.openmessaging.api.OnExceptionContext;
import io.openmessaging.api.Producer;
import io.openmessaging.api.SendCallback;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.Properties;
import org.apache.rocketmq.example.openmessaging.MqConfig;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.PropertyValueConst;

public class SimpleMQProducer {

    public static void main(String[] args) {

        Properties credentials = new Properties();
        credentials.setProperty(OMSBuiltinKeys.ACCESS_KEY, MqConfig.ACCESS_KEY);
        credentials.setProperty(OMSBuiltinKeys.SECRET_KEY, MqConfig.SECRET_KEY);

        MessagingAccessPoint accessPoint = OMS.builder()
            .withCredentials(credentials)
            .driver(MqConfig.DRIVER)
            .endpoint(MqConfig.ENDPOINT)
            .build();

        Properties producerProperties = new Properties();
        producerProperties.setProperty(PropertyKeyConst.GROUP_ID, MqConfig.GROUP_ID);
        producerProperties.setProperty(PropertyKeyConst.MSG_TRACE_SWITCH, "true");
        producerProperties.setProperty(PropertyKeyConst.ACL_ENABLE, "true");
        producerProperties.setProperty(PropertyKeyConst.ACCESS_CHANNEL, PropertyValueConst.CLOUD);

        Producer producer = accessPoint.createProducer(producerProperties);
        producer.start();
        System.out.printf("Producer Started %n");

        for (int i = 0; i < 10; i++) {
            Message message = new Message(MqConfig.TRANSACTION_TOPIC, "tagA", "mq send message test".getBytes());
            try {
                producer.sendAsync(message, new SendCallback() {
                    @Override public void onSuccess(SendResult result) {
                        System.out.printf("Send result: %s %n", result);
                    }

                    @Override public void onException(OnExceptionContext context) {
                        OMSRuntimeException clientException = context.getException();
                        clientException.getCause().printStackTrace();
                    }
                });
            } catch (OMSRuntimeException e) {
                e.printStackTrace();
            }
        }
    }
}
