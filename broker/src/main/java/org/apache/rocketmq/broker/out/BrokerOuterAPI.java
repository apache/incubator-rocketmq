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
package org.apache.rocketmq.broker.out;

import com.google.common.collect.Lists;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class BrokerOuterAPI {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    private String nameSrvAddr = null;
    private BrokerFixedThreadPoolExecutor brokerOuterExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
        new ArrayBlockingQueue<Runnable>(32), new ThreadFactoryImpl("brokerOutApi_thread_", true));

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(rpcHook);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.brokerOuterExecutor.shutdown();
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            lst.add(addr);
        }

        this.remotingClient.updateNameServerAddressList(lst);
    }

    public List<RegisterBrokerResult> registerBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final boolean oneway,
        final int timeoutMills,
        final boolean compressed) {

        final List<RegisterBrokerResult> registerBrokerResultList = Lists.newArrayList();
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {

            final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerId(brokerId);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setClusterName(clusterName);
            requestHeader.setHaServerAddr(haServerAddr);
            requestHeader.setCompressed(compressed);

            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
            requestBody.setFilterServerList(filterServerList);
            final byte[] body = requestBody.encode(compressed);
            final int bodyCrc32 = UtilAll.crc32(body);
            requestHeader.setBodyCrc32(bodyCrc32);
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            RegisterBrokerResult result = registerBroker(namesrvAddr,oneway, timeoutMills,requestHeader,body);
                            if (result != null) {
                                registerBrokerResultList.add(result);
                            }

                            log.info("register broker[{}]to name server {} OK", brokerId, namesrvAddr);
                        } catch (Exception e) {
                            log.warn("registerBroker Exception, {}", namesrvAddr, e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
            }

            try {
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
        }

        return registerBrokerResultList;
    }

    private RegisterBrokerResult registerBroker(
        final String namesrvAddr,
        final boolean oneway,
        final int timeoutMills,
        final RegisterBrokerRequestHeader requestHeader,
        final byte[] body
    ) throws RemotingCommandException, MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
        request.setBody(body);

        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, request, timeoutMills);
            } catch (RemotingTooMuchRequestException e) {
                // Ignore
            }
            return null;
        }

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                RegisterBrokerResponseHeader responseHeader =
                    (RegisterBrokerResponseHeader) response.decodeCommandCustomHeader(RegisterBrokerResponseHeader.class);
                RegisterBrokerResult result = new RegisterBrokerResult();
                result.setMasterAddr(responseHeader.getMasterAddr());
                result.setHaServerAddr(responseHeader.getHaServerAddr());
                if (response.getBody() != null) {
                    result.setKvTable(KVTable.decode(response.getBody(), KVTable.class));
                }
                return result;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void unregisterBrokerAll(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId
    ) {
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                    log.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                } catch (Exception e) {
                    log.warn("unregisterBroker Exception, {}", namesrvAddr, e);
                }
            }
        }
    }

    public void unregisterBroker(
        final String namesrvAddr,
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UnRegisterBrokerRequestHeader requestHeader = new UnRegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<Boolean> needRegister(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final int timeoutMills) {
        final List<Boolean> changedList = new CopyOnWriteArrayList<>();
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null && nameServerAddressList.size() > 0) {
            final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());
            for (final String namesrvAddr : nameServerAddressList) {
                brokerOuterExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
                            requestHeader.setBrokerAddr(brokerAddr);
                            requestHeader.setBrokerId(brokerId);
                            requestHeader.setBrokerName(brokerName);
                            requestHeader.setClusterName(clusterName);
                            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
                            request.setBody(topicConfigWrapper.getDataVersion().encode());
                            RemotingCommand response = remotingClient.invokeSync(namesrvAddr, request, timeoutMills);
                            DataVersion nameServerDataVersion = null;
                            Boolean changed = false;
                            switch (response.getCode()) {
                                case ResponseCode.SUCCESS: {
                                    QueryDataVersionResponseHeader queryDataVersionResponseHeader =
                                        (QueryDataVersionResponseHeader) response.decodeCommandCustomHeader(QueryDataVersionResponseHeader.class);
                                    changed = queryDataVersionResponseHeader.getChanged();
                                    byte[] body = response.getBody();
                                    if (body != null) {
                                        nameServerDataVersion = DataVersion.decode(body, DataVersion.class);
                                        if (!topicConfigWrapper.getDataVersion().equals(nameServerDataVersion)) {
                                            changed = true;
                                        }
                                    }
                                    if (changed == null || changed) {
                                        changedList.add(Boolean.TRUE);
                                    }
                                }
                                default:
                                    break;
                            }
                            log.warn("Query data version from name server {} OK,changed {}, broker {},name server {}", namesrvAddr, changed, topicConfigWrapper.getDataVersion(), nameServerDataVersion == null ? "" : nameServerDataVersion);
                        } catch (Exception e) {
                            changedList.add(Boolean.TRUE);
                            log.error("Query data version from name server {}  Exception, {}", namesrvAddr, e);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });

            }
            try {
                countDownLatch.await(timeoutMills, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("query dataversion from nameserver countDownLatch await Exception", e);
            }
        }
        return changedList;
    }

    public TopicConfigSerializeWrapper getAllTopicConfig(
        final String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(true, addr), request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ConsumerOffsetSerializeWrapper getAllConsumerOffset(
        final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerOffsetSerializeWrapper.decode(response.getBody(), ConsumerOffsetSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public String getAllDelayOffset(
        final String addr) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(response.getBody(), MixAll.DEFAULT_CHARSET);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(
        final String addr) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }
}
