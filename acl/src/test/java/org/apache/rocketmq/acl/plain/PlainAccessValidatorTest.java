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
package org.apache.rocketmq.acl.plain;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.*;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PlainAccessValidatorTest {

    private PlainAccessValidator plainAccessValidator;
    private AclClientRPCHook aclClient;
    private SessionCredentials sessionCredentials;
    @Before
    public void init() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl.yml");
        plainAccessValidator = new PlainAccessValidator();
        sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ");
        sessionCredentials.setSecretKey("12345678");
        sessionCredentials.setSecurityToken("87654321");
        aclClient = new AclClientRPCHook(sessionCredentials);
    }

    @Test
    public void contentTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "127.0.0.1");
        String signature = AclUtils.calSignature(accessResource.getContent(), sessionCredentials.getSecretKey());

        Assert.assertEquals(accessResource.getSignature(), signature);

    }

    @Test
    public void validateTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1");
        plainAccessValidator.validate(accessResource);

    }

    @Test
    public void validateSendMessageTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateSendMessageV2Test() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicC");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(messageRequestHeader));
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateForAdminCommandWithOutAclRPCHook() {
        RemotingCommand consumerOffsetAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        plainAccessValidator.parse(consumerOffsetAdminRequest, "192.168.0.1:9876");

        RemotingCommand subscriptionGroupAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        plainAccessValidator.parse(subscriptionGroupAdminRequest, "192.168.0.1:9876");

        RemotingCommand delayOffsetAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        plainAccessValidator.parse(delayOffsetAdminRequest, "192.168.0.1:9876");

        RemotingCommand allTopicConfigAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);
        plainAccessValidator.parse(allTopicConfigAdminRequest, "192.168.0.1:9876");

    }

    @Test
    public void validatePullMessageTest() {
        PullMessageRequestHeader pullMessageRequestHeader=new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topicC");
        pullMessageRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE,pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateConsumeMessageBackTest() {
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader=new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setOriginTopic("topicC");
        consumerSendMsgBackRequestHeader.setGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK,consumerSendMsgBackRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateQueryMessageTest() {
        QueryMessageRequestHeader queryMessageRequestHeader=new QueryMessageRequestHeader();
        queryMessageRequestHeader.setTopic("topicC");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE,queryMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateHeartBeatTest() {
        HeartbeatData heartbeatData=new HeartbeatData();
        Set<ProducerData> producerDataSet=new HashSet<>();
        Set<ConsumerData> consumerDataSet=new HashSet<>();
        Set<SubscriptionData> subscriptionDataSet=new HashSet<>();
        ProducerData producerData=new ProducerData();
        producerData.setGroupName("producerGroupA");
        ConsumerData consumerData=new ConsumerData();
        consumerData.setGroupName("consumerGroupA");
        SubscriptionData subscriptionData=new SubscriptionData();
        subscriptionData.setTopic("topicC");
        producerDataSet.add(producerData);
        consumerDataSet.add(consumerData);
        subscriptionDataSet.add(subscriptionData);
        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        heartbeatData.setProducerDataSet(producerDataSet);
        heartbeatData.setConsumerDataSet(consumerDataSet);
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT,null);
        remotingCommand.setBody(heartbeatData.encode());
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encode();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateUnRegisterClientTest() {
        UnregisterClientRequestHeader unregisterClientRequestHeader=new UnregisterClientRequestHeader();
        unregisterClientRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT,unregisterClientRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateGetConsumerListByGroupTest() {
        GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader=new GetConsumerListByGroupRequestHeader();
        getConsumerListByGroupRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP,getConsumerListByGroupRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateUpdateConsumerOffSetTest() {
        UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader=new UpdateConsumerOffsetRequestHeader();
        updateConsumerOffsetRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET,updateConsumerOffsetRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
        plainAccessValidator.validate(accessResource);
    }

    @Test(expected = AclException.class)
    public void validateNullAccessKeyTest() {
        SessionCredentials sessionCredentials=new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ1");
        sessionCredentials.setSecretKey("1234");
        AclClientRPCHook aclClientRPCHook=new AclClientRPCHook(sessionCredentials);
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClientRPCHook.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.1.1");
        plainAccessValidator.validate(accessResource);
    }

    @Test(expected = AclException.class)
    public void validateErrorSecretKeyTest() {
        SessionCredentials sessionCredentials=new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ");
        sessionCredentials.setSecretKey("1234");
        AclClientRPCHook aclClientRPCHook=new AclClientRPCHook(sessionCredentials);
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClientRPCHook.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.1.1");
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void validateGetAllTopicConfigTest() {
        String whiteRemoteAddress = "192.168.0.1";
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), whiteRemoteAddress);
        plainAccessValidator.validate(accessResource);
    }

    @Test
    public void updateAccessAclYamlConfigNormalTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_update_create.yml");

        String targetFileName = "src/test/resources/conf/plain_acl_update_create.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("RocketMQ");
        plainAccessConfig.setSecretKey("1234567890");
        plainAccessConfig.setDefaultGroupPerm("PUB");
        plainAccessConfig.setDefaultTopicPerm("SUB");
        List<String> topicPerms = new ArrayList<String>();
        topicPerms.add("topicC=PUB|SUB");
        topicPerms.add("topicB=PUB");
        plainAccessConfig.setTopicPerms(topicPerms);
        List<String> groupPerms = new ArrayList<String>();
        groupPerms.add("groupB=PUB|SUB");
        groupPerms.add("groupC=DENY");
        plainAccessConfig.setGroupPerms(groupPerms);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        // Update acl access yaml config file
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<Map<String, Object>> accounts =  (List<Map<String, Object>>)readableMap.get("accounts");
        Map<String, Object> verifyMap = null;
        for (Map<String, Object> account : accounts) {
            if (account.get("accessKey").equals(plainAccessConfig.getAccessKey())) {
                verifyMap = account;
                break;
            }
        }

        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY),"1234567890");
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_TOPIC_PERM),"SUB");
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_GROUP_PERM),"PUB");
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE),false);
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR),"192.168.0.*");
        Assert.assertEquals(((List)verifyMap.get(AclConstants.CONFIG_TOPIC_PERMS)).size(),2);
        Assert.assertEquals(((List)verifyMap.get(AclConstants.CONFIG_GROUP_PERMS)).size(),2);

        // Verify the dateversion element is correct or not
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get("dataVersion");
        Assert.assertEquals(1,dataVersions.get(0).get("counter"));

        // Restore the backup file and flush to yaml file
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    public void updateAccessAclYamlConfigTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_update_create.yml");

        String targetFileName = "src/test/resources/conf/plain_acl_update_create.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("RocketMQ");
        plainAccessConfig.setSecretKey("123456789111");

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        // Update element in the acl access yaml config file
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<Map<String, Object>> accounts =  (List<Map<String, Object>>)readableMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> verifyMap = null;
        for (Map<String, Object> account : accounts) {
            if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                verifyMap = account;
                break;
            }
        }
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY),"123456789111");

        // Restore the backup file and flush to yaml file
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }


    @Test
    public void createAndUpdateAccessAclYamlConfigNormalTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_update_create.yml");

        String targetFileName = "src/test/resources/conf/plain_acl_update_create.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("RocketMQ33");
        plainAccessConfig.setSecretKey("123456789111");
        plainAccessConfig.setDefaultGroupPerm("PUB");
        plainAccessConfig.setDefaultTopicPerm("DENY");
        List<String> topicPerms = new ArrayList<String>();
        topicPerms.add("topicC=PUB|SUB");
        topicPerms.add("topicB=PUB");
        plainAccessConfig.setTopicPerms(topicPerms);
        List<String> groupPerms = new ArrayList<String>();
        groupPerms.add("groupB=PUB|SUB");
        groupPerms.add("groupC=DENY");
        plainAccessConfig.setGroupPerms(groupPerms);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        // Create element in the acl access yaml config file
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<Map<String, Object>> accounts =  (List<Map<String, Object>>)readableMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> verifyMap = null;
        for (Map<String, Object> account : accounts) {
            if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                verifyMap = account;
                break;
            }
        }
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY),"123456789111");
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_TOPIC_PERM),"DENY");
        Assert.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_GROUP_PERM),"PUB");
        Assert.assertEquals(((List)verifyMap.get(AclConstants.CONFIG_TOPIC_PERMS)).size(),2);
        Assert.assertEquals(((List)verifyMap.get(AclConstants.CONFIG_GROUP_PERMS)).size(),2);
        Assert.assertTrue(((List)verifyMap.get(AclConstants.CONFIG_TOPIC_PERMS)).contains("topicC=PUB|SUB"));
        Assert.assertTrue(((List)verifyMap.get(AclConstants.CONFIG_TOPIC_PERMS)).contains("topicB=PUB"));
        Assert.assertTrue(((List)verifyMap.get(AclConstants.CONFIG_GROUP_PERMS)).contains("groupB=PUB|SUB"));
        Assert.assertTrue(((List)verifyMap.get(AclConstants.CONFIG_GROUP_PERMS)).contains("groupC=DENY"));

        // Verify the dateversion element is correct or not
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assert.assertEquals(1,dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));

        // Update element in the acl config yaml file
        PlainAccessConfig plainAccessConfig2 = new PlainAccessConfig();
        plainAccessConfig2.setAccessKey("rocketmq2");
        plainAccessConfig2.setSecretKey("1234567890123");

        // Update acl access yaml config file secondly
        plainAccessValidator.updateAccessConfig(plainAccessConfig2);

        Map<String, Object> readableMap2 = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<Map<String, Object>> accounts2 =  (List<Map<String, Object>>)readableMap2.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> verifyMap2 = null;
        for (Map<String, Object> account : accounts2) {
            if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig2.getAccessKey())) {
                verifyMap2 = account;
                break;
            }
        }

        // Verify the dateversion element after updating is correct or not
        List<Map<String, Object>> dataVersions2 = (List<Map<String, Object>>) readableMap2.get(AclConstants.CONFIG_DATA_VERSION);
        Assert.assertEquals(2,dataVersions2.get(0).get(AclConstants.CONFIG_COUNTER));
        Assert.assertEquals(verifyMap2.get(AclConstants.CONFIG_SECRET_KEY),"1234567890123");


        // Restore the backup file and flush to yaml file
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test(expected = AclException.class)
    public void updateAccessAclYamlConfigExceptionTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_update_create.yml");

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("RocketMQ");
        plainAccessConfig.setSecretKey("12345");

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        // Update acl access yaml config file
        plainAccessValidator.updateAccessConfig(plainAccessConfig);
    }

    @Test
    public void deleteAccessAclYamlConfigNormalTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_delete.yml");

        String targetFileName = "src/test/resources/conf/plain_acl_delete.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);


        String accessKey = "rocketmq2";
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.deleteAccessConfig(accessKey);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<Map<String, Object>> accounts =  (List<Map<String, Object>>)readableMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> verifyMap = null;
        for (Map<String, Object> account : accounts) {
            if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(accessKey)) {
                verifyMap = account;
                break;
            }
        }

        // Verify the specified element is removed or not
        Assert.assertEquals(verifyMap,null);
        // Verify the dateversion element is correct or not
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assert.assertEquals(1,dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));
        
        // Restore the backup file and flush to yaml file
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    public void updateAccessAclYamlConfigWithNoAccoutsExceptionTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_with_no_accouts.yml");

        String targetFileName = "src/test/resources/conf/plain_acl_with_no_accouts.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("RocketMQ");
        plainAccessConfig.setSecretKey("1234567890");

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        // Update acl access yaml config file and verify the return value is true
        Assert.assertEquals(plainAccessValidator.updateAccessConfig(plainAccessConfig), false);
    }

    @Test
    public void updateGlobalWhiteAddrsNormalTest() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl_global_white_addrs.yml");

        String targetFileName = "src/test/resources/conf/plain_acl_global_white_addrs.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        // Update global white remote addr value list in the acl access yaml config file

        List<String> globalWhiteAddrsList = new ArrayList<String>();
        globalWhiteAddrsList.add("10.10.154.1");
        globalWhiteAddrsList.add("10.10.154.2");
        globalWhiteAddrsList.add("10.10.154.3");
        plainAccessValidator.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        List<String> globalWhiteAddrList =  (List<String>)readableMap.get(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        Assert.assertTrue(globalWhiteAddrList.contains("10.10.154.1"));
        Assert.assertTrue(globalWhiteAddrList.contains("10.10.154.2"));
        Assert.assertTrue(globalWhiteAddrList.contains("10.10.154.3"));

        // Verify the dateversion element is correct or not
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assert.assertEquals(1,dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));

        // Restore the backup file and flush to yaml file
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    public void getAllAclConfigTest(){
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        Assert.assertEquals(aclConfig.getGlobalWhiteAddrs().size(), 2);
        Assert.assertEquals(aclConfig.getPlainAccessConfigs().size(), 2);
    }

}
