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
package org.apache.rocketmq.client.producer;

import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.RPCHook;

public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;
    private int checkThreadPoolMinSize = 1;
    private int checkThreadPoolMaxSize = 1;
    private int checkRequestHoldMax = 2000;

    private ExecutorService executorService;

    private TransactionListener transactionListener;

    public TransactionMQProducer() {
    }

    public TransactionMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    public TransactionMQProducer(final String namespace, final String producerGroup) {
        this(namespace, producerGroup, null);
    }

    public TransactionMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(null, producerGroup, rpcHook);
    }

    public TransactionMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        super(namespace, producerGroup, rpcHook);
    }

    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.initTransactionEnv();
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.defaultMQProducerImpl.destroyTransactionEnv();
    }

    /**
     * This method will be removed in the version 5.0.0, method <code>sendMessageInTransaction(Message,Object)</code>}
     * is recommended.
     */
    @Override
    @Deprecated
    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException {
        if (null == this.transactionCheckListener) {
            throw new MQClientException("localTransactionBranchCheckListener is null", null);
        }

        msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, tranExecuter, arg);
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final Object arg) throws MQClientException {
        if (null == this.transactionListener) {
            throw new MQClientException("TransactionListener is null", null);
        }

        msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, null, arg);
    }

    public TransactionCheckListener getTransactionCheckListener() {
        return transactionCheckListener;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener) {
        this.transactionCheckListener = transactionCheckListener;
    }

    public int getCheckThreadPoolMinSize() {
        return checkThreadPoolMinSize;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setCheckThreadPoolMinSize(int checkThreadPoolMinSize) {
        this.checkThreadPoolMinSize = checkThreadPoolMinSize;
    }

    public int getCheckThreadPoolMaxSize() {
        return checkThreadPoolMaxSize;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setCheckThreadPoolMaxSize(int checkThreadPoolMaxSize) {
        this.checkThreadPoolMaxSize = checkThreadPoolMaxSize;
    }

    public int getCheckRequestHoldMax() {
        return checkRequestHoldMax;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setCheckRequestHoldMax(int checkRequestHoldMax) {
        this.checkRequestHoldMax = checkRequestHoldMax;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;
    }
}
