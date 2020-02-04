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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * This class is the entry point for applications intending to send messages. </p>
 *
 * It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well out of
 * box for most scenarios. </p>
 *
 * This class aggregates various <code>send</code> methods to deliver messages to brokers. Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding. </p>
 *
 * <p> <strong>Thread Safety:</strong> After configuring and starting process, this class can be regarded as thread-safe
 * and used among multiple threads context. </p>
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;
    private final InternalLogger log = ClientLogger.getLog();
    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved. </p>
     *
     * For non-transactional messages, it does not matter as long as it's unique per process. </p>
     *
     * See {@linktourl http://rocketmq.apache.org/docs/core-concept/} for more discussion.
     */
    private String producerGroup;

    /**
     * Just for testing or demo program
     */
    private String createTopicKey = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * Number of queues to create per default topic.
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     */
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode. </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode. </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message size in bytes.
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * Interface of asynchronous transfer data
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * Default constructor.
     */
    public DefaultMQProducer() {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * Constructor specifying the RPC hook.
     *
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
     * trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,
        final String customizedTraceTopic) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //if client open the message trace feature
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);
                dispatcher.setHostProducer(this.defaultMQProducerImpl);
                traceDispatcher = dispatcher;
                this.defaultMQProducerImpl.registerSendMessageHook(
                    new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * Constructor specifying producer group.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup) {
        this(namespace, producerGroup, null);
    }

    /**
     * Constructor specifying both producer group and RPC hook.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(null, producerGroup, rpcHook);
    }

    /**
     * Constructor specifying namespace, producer group and RPC hook.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying producer group and enabled msg trace flag.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param enableMsgTrace Switch flag instance for message trace.
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace) {
        this(null, producerGroup, null, enableMsgTrace, null);
    }

    /**
     * Constructor specifying producer group, enabled msgTrace flag and customized trace topic name.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
     * trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(null, producerGroup, null, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying namespace, producer group, RPC hook, enabled msgTrace flag and customized trace topic
     * name.
     *
     * @param namespace Namespace for this MQ Producer instance.
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default
     * trace topic name.
     */
    public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook,
        boolean enableMsgTrace, final String customizedTraceTopic) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //if client open the message trace feature
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, customizedTraceTopic, rpcHook);
                dispatcher.setHostProducer(this.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.getDefaultMQProducerImpl().registerSendMessageHook(
                    new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * Start this producer instance. </p>
     *
     * <strong> Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must
     * to invoke this method before sending or querying messages. </strong> </p>
     *
     * @throws MQClientException if there is any unexpected error.
     */
    @Override
    public void start() throws MQClientException {
        this.setProducerGroup(withNamespace(this.producerGroup));
        this.defaultMQProducerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally completes. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Validators.checkMessage(msg, this);
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg);
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     *
     * @param msg Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * Send message to broker asynchronously. </p>
     *
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed. </p>
     *
     * Similar to {@link #send(Message)}, internal implementation would potentially retry up to {@link
     * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
     * application developers are the one to resolve this potential issue.
     *
     * @param msg Message to send.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
     *
     * @param msg message to send.
     * @param sendCallback Callback to execute.
     * @param timeout send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     *
     * @param msg Message to send.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with target message queue specified in addition.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @param timeout Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message selector through which to get target message queue.
     * @param arg Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object, SendCallback)} with timeout specified.
     *
     * @param msg Message to send.
     * @param selector Message selector through which to get target message queue.
     * @param arg Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * Send request message in synchronous mode. This method returns only when the consumer consume the request message and reply a message. </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg request message to send
     * @param timeout request timeout
     * @return reply message
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
        RemotingException, MQBrokerException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, timeout);
    }

    /**
     * Request asynchronously. </p>
     * This method returns immediately. On receiving reply message, <code>requestCallback</code> will be executed. </p>
     *
     * Similar to {@link #request(Message, long)}, internal implementation would potentially retry up to {@link
     * #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication and
     * application developers are the one to resolve this potential issue.
     *
     * @param msg request message to send
     * @param requestCallback callback to execute on request completion.
     * @param timeout request timeout
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final RequestCallback requestCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with message queue selector specified.
     *
     * @param msg request message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param timeout timeout of request.
     * @return reply message
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message selector specified.
     *
     * @param msg requst message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param requestCallback callback to execute on request completion.
     * @param timeout timeout of request.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout) throws MQClientException, RemotingException,
        InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, selector, arg, requestCallback, timeout);
    }

    /**
     * Same to {@link #request(Message, long)}  with target message queue specified in addition.
     *
     * @param msg request message to send
     * @param mq target message queue.
     * @param timeout request timeout
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws RequestTimeoutException if request timeout.
     */
    @Override
    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, mq, timeout);
    }

    /**
     * Same to {@link #request(Message, RequestCallback, long)} with target message queue specified.
     *
     * @param msg request message to send
     * @param mq target message queue.
     * @param requestCallback callback to execute on request completion.
     * @param timeout timeout of request.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQBrokerException if there is any broker error.
     */
    @Override
    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg Argument used along with message queue selector.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * This method is to send transactional messages.
     *
     * @param msg Transactional message to send.
     * @param tranExecuter local transaction executor.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
        final Object arg)
        throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * This method is used to send transactional messages.
     *
     * @param msg Transactional message to send.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg,
        Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    /**
     * Create a topic on broker. This method will be removed in a certain version after April 5, 2020, so please do not
     * use this method.
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @param topicSysFlag topic system flag
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * Search consume queue offset of the given time stamp.
     *
     * @param mq Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return Consume queue offset.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * Query maximum offset of the given message queue.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return maximum offset of the given consume queue.
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * Query minimum offset of the given message queue.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return minimum offset of the given message queue.
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * Query earliest message store time.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return earliest message store time.
     * @throws MQClientException if there is any client error.
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * Query message of the given offset message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param offsetMsgId message id
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * Query message by key.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic message topic
     * @param key message key index word
     * @param maxNum max message number
     * @param begin from when
     * @param end to when
     * @return QueryResult instance contains matched messages.
     * @throws MQClientException if there is any client error.
     * @throws InterruptedException if the thread is interrupted.
     */
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * Query message of the given message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    @Override
    public SendResult send(
        Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs,
        MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods. If the Executor is not set, {@link
     * NettyRemotingClient#publicExecutor} will be used.
     *
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
    }

    /**
     * Sets an Executor to be used for executing asynchronous send. If the Executor is not set, {@link
     * DefaultMQProducerImpl#defaultAsyncSenderExecutor} will be used.
     *
     * @param asyncSenderExecutor the instance of Executor
     */
    public void setAsyncSenderExecutor(final ExecutorService asyncSenderExecutor) {
        this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
                message.setTopic(withNamespace(message.getTopic()));
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    @Deprecated
    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

}
