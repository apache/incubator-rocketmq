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

package io.openmessaging.rocketmq;

import io.openmessaging.api.Consumer;
import io.openmessaging.api.Message;
import io.openmessaging.api.MessagingAccessPoint;
import io.openmessaging.api.Producer;
import io.openmessaging.api.PullConsumer;
import io.openmessaging.api.batch.BatchConsumer;
import io.openmessaging.api.order.OrderConsumer;
import io.openmessaging.api.order.OrderProducer;
import io.openmessaging.api.transaction.LocalTransactionChecker;
import io.openmessaging.api.transaction.TransactionProducer;
import io.openmessaging.api.transaction.TransactionStatus;
import java.util.Properties;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.oms.api.Constants;
import org.apache.rocketmq.oms.api.impl.MQClientInfo;
import org.apache.rocketmq.oms.api.impl.rocketmq.BatchConsumerImpl;
import org.apache.rocketmq.oms.api.impl.rocketmq.ConsumerImpl;
import org.apache.rocketmq.oms.api.impl.rocketmq.OMSUtil;
import org.apache.rocketmq.oms.api.impl.rocketmq.OrderConsumerImpl;
import org.apache.rocketmq.oms.api.impl.rocketmq.OrderProducerImpl;
import org.apache.rocketmq.oms.api.impl.rocketmq.ProducerImpl;
import org.apache.rocketmq.oms.api.impl.rocketmq.PullConsumerImpl;
import org.apache.rocketmq.oms.api.impl.rocketmq.TransactionProducerImpl;

public class MessagingAccessPointImpl implements MessagingAccessPoint {

    private Properties attributes;

    public MessagingAccessPointImpl(Properties attributes) {
        this.attributes = attributes;
    }

    @Override
    public String version() {
        return MQClientInfo.currentVersion;
    }

    @Override public Properties attributes() {
        return null;
    }

    @Override public PullConsumer createPullConsumer(Properties properties) {
        properties.putAll(attributes);
        return new PullConsumerImpl(OMSUtil.extractProperties(properties));
    }

    @Override
    public Producer createProducer(final Properties properties) {
        properties.putAll(attributes);
        return new ProducerImpl(OMSUtil.extractProperties(properties));
    }

    @Override
    public Consumer createConsumer(final Properties properties) {
        properties.putAll(attributes);
        return new ConsumerImpl(OMSUtil.extractProperties(properties));
    }

    @Override
    public BatchConsumer createBatchConsumer(final Properties properties) {
        properties.putAll(attributes);
        return new BatchConsumerImpl(OMSUtil.extractProperties(properties));
    }

    @Override
    public OrderProducer createOrderProducer(final Properties properties) {
        properties.putAll(attributes);
        return new OrderProducerImpl(OMSUtil.extractProperties(properties));
    }

    @Override
    public OrderConsumer createOrderedConsumer(final Properties properties) {
        properties.putAll(attributes);
        return new OrderConsumerImpl(OMSUtil.extractProperties(properties));
    }

    @Override
    public TransactionProducer createTransactionProducer(Properties properties,
        final LocalTransactionChecker checker) {
        properties.putAll(attributes);
        return new TransactionProducerImpl(OMSUtil.extractProperties(properties), new TransactionCheckListener() {
            @Override
            public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                String msgId = msg.getProperty(Constants.TRANSACTION_ID);
                Message message = OMSUtil.msgConvert(msg);
                message.setMsgID(msgId);
                TransactionStatus check = checker.check(message);
                if (TransactionStatus.CommitTransaction == check) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (TransactionStatus.RollbackTransaction == check) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return LocalTransactionState.UNKNOW;
            }
        });
    }
}
