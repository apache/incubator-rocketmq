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
package org.apache.rocketmq.tools.admin;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.api.MessageTrack;

public interface MQAdminExt extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();

    void updateBrokerConfig(final String brokerAddr, final Properties properties) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    Properties getBrokerConfig(final String brokerAddr) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

    void createAndUpdateTopicConfig(final String addr,
        final TopicConfig config) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void createAndUpdatePlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deletePlainAccessConfig(final String addr, final String accessKey) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void updateGlobalWhiteAddrConfig(final String addr, final String globalWhiteAddrs)throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    ClusterAclVersionInfo examineBrokerClusterAclVersionInfo(final String addr) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    AclConfig examineBrokerClusterAclConfig(final String addr) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void createAndUpdateSubscriptionGroupConfig(final String addr,
        final SubscriptionGroupConfig config) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    SubscriptionGroupConfig examineSubscriptionGroupConfig(final String addr, final String group);

    TopicConfig examineTopicConfig(final String addr, final String topic);

    TopicStatsTable examineTopicStats(
        final String topic) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException;

    TopicList fetchTopicsByCLuster(
        String clusterName) throws RemotingException, MQClientException, InterruptedException;

    KVTable fetchBrokerRuntimeStats(
        final String brokerAddr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException;

    ConsumeStats examineConsumeStats(
        final String consumerGroup) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    ConsumeStats examineConsumeStats(final String consumerGroup,
        final String topic) throws RemotingException, MQClientException,
        InterruptedException, MQBrokerException;

    ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException;

    TopicRouteData examineTopicRouteInfo(
        final String topic) throws RemotingException, MQClientException, InterruptedException;

    ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException,
        MQClientException;

    ProducerConnection examineProducerConnectionInfo(final String producerGroup,
        final String topic) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException;

    List<String> getNameServerAddressList();

    int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException;

    void putKVConfig(final String namespace, final String key, final String value);

    String getKVConfig(final String namespace,
        final String key) throws RemotingException, MQClientException, InterruptedException;

    KVTable getKVListByNamespace(
        final String namespace) throws RemotingException, MQClientException, InterruptedException;

    void deleteTopicInBroker(final Set<String> addrs, final String topic) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteTopicInNameServer(final Set<String> addrs,
        final String topic) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteSubscriptionGroup(final String addr, String groupName) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void createAndUpdateKvConfig(String namespace, String key,
        String value) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    void deleteKvConfig(String namespace, String key) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException;

    List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp, boolean force)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group,
        String clientAddr) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    void createOrUpdateOrderConf(String key, String value,
        boolean isCluster) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    GroupList queryTopicConsumeByWho(final String topic) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingException, MQClientException;

    List<QueueTimeSpan> queryConsumeTimeSpan(final String topic,
        final String group) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException;

    boolean cleanExpiredConsumerQueue(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanExpiredConsumerQueueByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanUnusedTopic(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    boolean cleanUnusedTopicByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId, final boolean jstack)
        throws RemotingException, MQClientException, InterruptedException;

    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
        String clientId,
        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup,
        String clientId,
        String topic,
        String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    List<MessageTrack> messageTrackDetail(
        MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException;

    void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException;

    BrokerStatsData viewBrokerStatsData(final String brokerAddr, final String statsName, final String statsKey)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQClientException,
        InterruptedException;

    Set<String> getClusterList(final String topic) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder,
        long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException;

    Set<String> getTopicClusterList(
        final String topic) throws InterruptedException, MQBrokerException, MQClientException, RemotingException;

    SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException;

    TopicConfigSerializeWrapper getAllTopicGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException;

    void updateConsumeOffset(String brokerAddr, String consumeGroup, MessageQueue mq,
        long offset) throws RemotingException, InterruptedException, MQBrokerException;

    /**
     * Update name server config.
     * <br>
     * Command Code : RequestCode.UPDATE_NAMESRV_CONFIG
     *
     * <br> If param(nameServers) is null or empty, will use name servers from ns!
     */
    void updateNameServerConfig(final Properties properties,
        final List<String> nameServers) throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, RemotingSendRequestException, RemotingTimeoutException,
        MQClientException, MQBrokerException;

    /**
     * Get name server config.
     * <br>
     * Command Code : RequestCode.GET_NAMESRV_CONFIG
     * <br> If param(nameServers) is null or empty, will use name servers from ns!
     *
     * @return The fetched name server config
     */
    Map<String, Properties> getNameServerConfig(final List<String> nameServers) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQClientException, UnsupportedEncodingException;

    /**
     * query consume queue data
     *
     * @param brokerAddr broker ip address
     * @param topic topic
     * @param queueId id of queue
     * @param index start offset
     * @param count how many
     * @param consumerGroup group
     */
    QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr,
        final String topic, final int queueId,
        final long index, final int count, final String consumerGroup)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException;

    boolean resumeCheckHalfMessage(String msgId)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

    boolean resumeCheckHalfMessage(final String topic, final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;
}
