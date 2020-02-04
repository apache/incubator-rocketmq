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
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.MixAll;
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
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.api.MessageTrack;

public class DefaultMQAdminExt extends ClientConfig implements MQAdminExt {
    private final DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private String adminExtGroup = "admin_ext_group";
    private String createTopicKey = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
    private long timeoutMillis = 5000;

    public DefaultMQAdminExt() {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, null, timeoutMillis);
    }

    public DefaultMQAdminExt(long timeoutMillis) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, null, timeoutMillis);
    }

    public DefaultMQAdminExt(RPCHook rpcHook) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, rpcHook, timeoutMillis);
    }

    public DefaultMQAdminExt(RPCHook rpcHook, long timeoutMillis) {
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, rpcHook, timeoutMillis);
    }

    public DefaultMQAdminExt(final String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, timeoutMillis);
    }

    public DefaultMQAdminExt(final String adminExtGroup, long timeoutMillis) {
        this.adminExtGroup = adminExtGroup;
        this.defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(this, timeoutMillis);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        defaultMQAdminExtImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return defaultMQAdminExtImpl.searchOffset(mq, timestamp);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.maxOffset(mq);
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.minOffset(mq);
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return defaultMQAdminExtImpl.earliestMsgStoreTime(mq);
    }

    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin,
        long end) throws MQClientException,
        InterruptedException {
        return defaultMQAdminExtImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public void start() throws MQClientException {
        defaultMQAdminExtImpl.start();
    }

    @Override
    public void shutdown() {
        defaultMQAdminExtImpl.shutdown();
    }

    @Override
    public void updateBrokerConfig(String brokerAddr,
        Properties properties) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException {
        defaultMQAdminExtImpl.updateBrokerConfig(brokerAddr, properties);
    }

    @Override
    public Properties getBrokerConfig(final String brokerAddr) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.getBrokerConfig(brokerAddr);
    }

    @Override
    public void createAndUpdateTopicConfig(String addr, TopicConfig config) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateTopicConfig(addr, config);
    }

    @Override
    public void createAndUpdatePlainAccessConfig(String addr,
        PlainAccessConfig config) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdatePlainAccessConfig(addr, config);
    }

    @Override public void deletePlainAccessConfig(String addr,
        String accessKey) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.deletePlainAccessConfig(addr, accessKey);
    }

    @Override public void updateGlobalWhiteAddrConfig(String addr,
        String globalWhiteAddrs) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.updateGlobalWhiteAddrConfig(addr, globalWhiteAddrs);
    }

    @Override public ClusterAclVersionInfo examineBrokerClusterAclVersionInfo(
        String addr) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.examineBrokerClusterAclVersionInfo(addr);
    }

    @Override public AclConfig examineBrokerClusterAclConfig(
        String addr) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.examineBrokerClusterAclConfig(addr);
    }

    @Override
    public void createAndUpdateSubscriptionGroupConfig(String addr,
        SubscriptionGroupConfig config) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateSubscriptionGroupConfig(addr, config);
    }

    @Override
    public SubscriptionGroupConfig examineSubscriptionGroupConfig(String addr, String group) {
        return defaultMQAdminExtImpl.examineSubscriptionGroupConfig(addr, group);
    }

    @Override
    public TopicConfig examineTopicConfig(String addr, String topic) {
        return defaultMQAdminExtImpl.examineTopicConfig(addr, topic);
    }

    @Override
    public TopicStatsTable examineTopicStats(
        String topic) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return defaultMQAdminExtImpl.examineTopicStats(topic);
    }

    @Override
    public TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchAllTopicList();
    }

    @Override
    public TopicList fetchTopicsByCLuster(
        String clusterName) throws RemotingException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchTopicsByCLuster(clusterName);
    }

    @Override
    public KVTable fetchBrokerRuntimeStats(
        final String brokerAddr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.fetchBrokerRuntimeStats(brokerAddr);
    }

    @Override
    public ConsumeStats examineConsumeStats(
        String consumerGroup) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return examineConsumeStats(consumerGroup, null);
    }

    @Override
    public ConsumeStats examineConsumeStats(String consumerGroup,
        String topic) throws RemotingException, MQClientException,
        InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineConsumeStats(consumerGroup, topic);
    }

    @Override
    public ClusterInfo examineBrokerClusterInfo() throws InterruptedException, RemotingConnectException, RemotingTimeoutException,
        RemotingSendRequestException, MQBrokerException {
        return defaultMQAdminExtImpl.examineBrokerClusterInfo();
    }

    @Override
    public TopicRouteData examineTopicRouteInfo(
        String topic) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.examineTopicRouteInfo(topic);
    }

    @Override
    public ConsumerConnection examineConsumerConnectionInfo(
        String consumerGroup) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException {
        return defaultMQAdminExtImpl.examineConsumerConnectionInfo(consumerGroup);
    }

    @Override
    public ProducerConnection examineProducerConnectionInfo(String producerGroup,
        final String topic) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.examineProducerConnectionInfo(producerGroup, topic);
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.defaultMQAdminExtImpl.getNameServerAddressList();
    }

    @Override
    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.wipeWritePermOfBroker(namesrvAddr, brokerName);
    }

    @Override
    public void putKVConfig(String namespace, String key, String value) {
        defaultMQAdminExtImpl.putKVConfig(namespace, key, value);
    }

    @Override
    public String getKVConfig(String namespace,
        String key) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getKVConfig(namespace, key);
    }

    @Override
    public KVTable getKVListByNamespace(
        String namespace) throws RemotingException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getKVListByNamespace(namespace);
    }

    @Override
    public void deleteTopicInBroker(Set<String> addrs,
        String topic) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteTopicInBroker(addrs, topic);
    }

    @Override
    public void deleteTopicInNameServer(Set<String> addrs,
        String topic) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteTopicInNameServer(addrs, topic);
    }

    @Override
    public void deleteSubscriptionGroup(String addr,
        String groupName) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteSubscriptionGroup(addr, groupName);
    }

    @Override
    public void createAndUpdateKvConfig(String namespace, String key,
        String value) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createAndUpdateKvConfig(namespace, key, value);
    }

    @Override
    public void deleteKvConfig(String namespace,
        String key) throws RemotingException, MQBrokerException, InterruptedException,
        MQClientException {
        defaultMQAdminExtImpl.deleteKvConfig(namespace, key);
    }

    @Override
    public List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp,
        boolean force)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
    }

    @Override
    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return resetOffsetByTimestamp(topic, group, timestamp, isForce, false);
    }

    public Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean isForce,
        boolean isC)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.resetOffsetByTimestamp(topic, group, timestamp, isForce, isC);
    }

    @Override
    public void resetOffsetNew(String consumerGroup, String topic,
        long timestamp) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        this.defaultMQAdminExtImpl.resetOffsetNew(consumerGroup, topic, timestamp);
    }

    @Override
    public Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group,
        String clientAddr) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        return defaultMQAdminExtImpl.getConsumeStatus(topic, group, clientAddr);
    }

    @Override
    public void createOrUpdateOrderConf(String key, String value,
        boolean isCluster) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, isCluster);
    }

    @Override
    public GroupList queryTopicConsumeByWho(
        String topic) throws InterruptedException, MQBrokerException, RemotingException,
        MQClientException {
        return this.defaultMQAdminExtImpl.queryTopicConsumeByWho(topic);
    }

    @Override
    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic,
        final String group) throws InterruptedException, MQBrokerException,
        RemotingException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeTimeSpan(topic, group);
    }

    @Override
    public boolean cleanExpiredConsumerQueue(
        String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueue(cluster);
    }

    @Override
    public boolean cleanExpiredConsumerQueueByAddr(
        String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanExpiredConsumerQueueByAddr(addr);
    }

    @Override
    public boolean cleanUnusedTopic(String cluster) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanUnusedTopicByAddr(cluster);
    }

    @Override
    public boolean cleanUnusedTopicByAddr(String addr) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.cleanUnusedTopicByAddr(addr);
    }

    @Override
    public ConsumerRunningInfo getConsumerRunningInfo(String consumerGroup, String clientId,
        boolean jstack) throws RemotingException,
        MQClientException, InterruptedException {
        return defaultMQAdminExtImpl.getConsumerRunningInfo(consumerGroup, clientId, jstack);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String consumerGroup, String clientId, String msgId)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(consumerGroup, clientId, msgId);
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String consumerGroup, final String clientId,
        final String topic,
        final String msgId) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return defaultMQAdminExtImpl.consumeMessageDirectly(consumerGroup, clientId, topic, msgId);
    }

    @Override
    public List<MessageTrack> messageTrackDetail(
        MessageExt msg) throws RemotingException, MQClientException, InterruptedException,
        MQBrokerException {
        return this.defaultMQAdminExtImpl.messageTrackDetail(msg);
    }

    @Override
    public void cloneGroupOffset(String srcGroup, String destGroup, String topic,
        boolean isOffline) throws RemotingException,
        MQClientException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
    }

    @Override
    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName,
        String statsKey) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.viewBrokerStatsData(brokerAddr, statsName, statsKey);
    }

    @Override
    public Set<String> getClusterList(String topic) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.getClusterList(topic);
    }

    @Override
    public ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder,
        long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, MQClientException, InterruptedException {
        return this.defaultMQAdminExtImpl.fetchConsumeStatsInBroker(brokerAddr, isOrder, timeoutMillis);
    }

    @Override
    public Set<String> getTopicClusterList(
        final String topic) throws InterruptedException, MQBrokerException, MQClientException, RemotingException {
        return this.defaultMQAdminExtImpl.getTopicClusterList(topic);
    }

    @Override
    public SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getAllSubscriptionGroup(brokerAddr, timeoutMillis);
    }

    @Override
    public TopicConfigSerializeWrapper getAllTopicGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQBrokerException {
        return this.defaultMQAdminExtImpl.getAllTopicGroup(brokerAddr, timeoutMillis);
    }

    /* (non-Javadoc)
     * @see org.apache.rocketmq.client.MQAdmin#queryMessageByUniqKey(java.lang.String, java.lang.String)
     */
    @Override
    public MessageExt viewMessage(String topic, String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQAdminExtImpl.viewMessage(topic, msgId);
    }

    public String getAdminExtGroup() {
        return adminExtGroup;
    }

    public void setAdminExtGroup(String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    @Override
    public void updateConsumeOffset(String brokerAddr, String consumeGroup, MessageQueue mq,
        long offset) throws RemotingException, InterruptedException, MQBrokerException {
        this.defaultMQAdminExtImpl.updateConsumeOffset(brokerAddr, consumeGroup, mq, offset);
    }

    @Override
    public void updateNameServerConfig(final Properties properties, final List<String> nameServers)
        throws InterruptedException, RemotingConnectException,
        UnsupportedEncodingException, MQBrokerException, RemotingTimeoutException,
        MQClientException, RemotingSendRequestException {
        this.defaultMQAdminExtImpl.updateNameServerConfig(properties, nameServers);
    }

    @Override
    public Map<String, Properties> getNameServerConfig(final List<String> nameServers)
        throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQClientException,
        UnsupportedEncodingException {
        return this.defaultMQAdminExtImpl.getNameServerConfig(nameServers);
    }

    @Override
    public QueryConsumeQueueResponseBody queryConsumeQueue(String brokerAddr, String topic, int queueId, long index,
        int count, String consumerGroup)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
        return this.defaultMQAdminExtImpl.queryConsumeQueue(
            brokerAddr, topic, queueId, index, count, consumerGroup
        );
    }

    @Override
    public boolean resumeCheckHalfMessage(String msgId)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.resumeCheckHalfMessage(msgId);
    }

    @Override
    public boolean resumeCheckHalfMessage(String topic,
            String msgId)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        return this.defaultMQAdminExtImpl.resumeCheckHalfMessage(topic, msgId);
    }
}
