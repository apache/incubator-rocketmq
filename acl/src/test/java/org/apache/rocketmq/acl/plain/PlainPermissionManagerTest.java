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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class PlainPermissionManagerTest {

    PlainPermissionManager plainPermissionManager;
    PlainAccessResource PUBPlainAccessResource;
    PlainAccessResource SUBPlainAccessResource;
    PlainAccessResource ANYPlainAccessResource;
    PlainAccessResource DENYPlainAccessResource;
    PlainAccessResource plainAccessResource = new PlainAccessResource();
    PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
    Set<Integer> adminCode = new HashSet<>();

    static String DEFAULT_PLAIN_ACL_FILE_PATH;

    @BeforeClass
    public static void initBeforeClass() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.dir", "/conf/plain_acl");
        DEFAULT_PLAIN_ACL_FILE_PATH = System.getProperty("rocketmq.home.dir") + File.separator + System.getProperty("rocketmq.acl.plain.dir") + File.separator + "plain_acl_default.yml";
    }

    @Before
    public void init() throws Exception {
        // UPDATE_AND_CREATE_TOPIC
        adminCode.add(17);
        // UPDATE_BROKER_CONFIG
        adminCode.add(25);
        // DELETE_TOPIC_IN_BROKER
        adminCode.add(215);
        // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
        adminCode.add(200);
        // DELETE_SUBSCRIPTIONGROUP
        adminCode.add(207);

        PUBPlainAccessResource = clonePlainAccessResource(Permission.PUB);
        SUBPlainAccessResource = clonePlainAccessResource(Permission.SUB);
        ANYPlainAccessResource = clonePlainAccessResource(Permission.ANY);
        DENYPlainAccessResource = clonePlainAccessResource(Permission.DENY);

        System.setProperty("rocketmq.home.dir", "src/test/resources");

        plainPermissionManager = new PlainPermissionManager();
    }

    @Test
    public void testLoad() throws Exception {
        Field field = PlainPermissionManager.class.getDeclaredField("globalWhiteRemoteAddressStrategy");
        field.setAccessible(true);
        Map<String, RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = (Map<String, RemoteAddressStrategy>) field.get(plainPermissionManager);
        assertThat(globalWhiteRemoteAddressStrategy.size()).isEqualTo(3);
        assertThat(globalWhiteRemoteAddressStrategy.keySet()).containsExactlyInAnyOrder("1.1.1.1", "1.1.1.2", "1.1.2.*");

        field = PlainPermissionManager.class.getDeclaredField("akFilepathMap");
        field.setAccessible(true);
        Map<String, String> akFilepathMap = (Map<String, String>) field.get(plainPermissionManager);
        assertThat(akFilepathMap.entrySet().size()).isEqualTo(2);
        for (Map.Entry<String, String> entry : akFilepathMap.entrySet()) {
            String ak = entry.getKey();
            String filepath = entry.getValue();
            switch (ak) {
                case "RocketMQ":
                    assertThat(new File(filepath).getName()).isEqualTo("plain_acl_default.yml");
                    break;
                case "rocketmq2_acl_ak":
                    assertThat(new File(filepath).getName()).isEqualTo("plain_acl_rocketmq2.yml");
                    break;
            }
        }

        field = PlainPermissionManager.class.getDeclaredField("plainAccessResourceMap");
        field.setAccessible(true);
        Map<String, PlainAccessResource> plainAccessResourceMap = (Map<String, PlainAccessResource>) field.get(plainPermissionManager);
        assertThat(plainAccessResourceMap.entrySet().size()).isEqualTo(2);
        for (Map.Entry<String, PlainAccessResource> entry : plainAccessResourceMap.entrySet()) {
            String ak = entry.getKey();
            PlainAccessResource par = entry.getValue();
            assertThat(par.getAccessKey()).isEqualTo(ak);
            switch (ak) {
                case "RocketMQ":
                    assertThat(par.getSecretKey()).isEqualTo("12345678");
                    assertThat(par.getWhiteRemoteAddress()).isEqualTo("192.168.0.*");
                    assertThat(par.isAdmin()).isFalse();
                    assertThat(par.getDefaultTopicPerm()).isEqualTo(Permission.DENY);
                    assertThat(par.getDefaultGroupPerm()).isEqualTo(Permission.SUB);
                    assertThat(par.getResourcePermMap().size()).isEqualTo(5);
                    for (Map.Entry<String, Byte> permEntry : par.getResourcePermMap().entrySet()) {
                        String key = permEntry.getKey();
                        assertThat(key.equals("topicA") || key.equals("topicB") || key.equals("topicC") || key.equals("%RETRY%groupA") || key.equals("%RETRY%groupB")).isTrue();
                        Byte perm = permEntry.getValue();
                        switch (key) {
                            case "topicA":
                            case "%RETRY%groupA":
                                assertThat(perm).isEqualTo(Permission.DENY);
                                break;
                            case "topicB":
                                assertThat(perm).isEqualTo((byte) (Permission.PUB | Permission.SUB));
                                break;
                            case "topicC":
                            case "%RETRY%groupB":
                                assertThat(perm).isEqualTo(Permission.SUB);
                                break;
                        }
                    }
                    break;
                case "rocketmq2_acl_ak":
                    assertThat(par.getSecretKey()).isEqualTo("rocketmq2_acl_sk");
                    assertThat(par.getWhiteRemoteAddress()).isEqualTo("192.168.1.*");
                    assertThat(par.isAdmin()).isTrue();
                    break;
            }
        }

    }


    public PlainAccessResource clonePlainAccessResource(byte perm) {
        PlainAccessResource painAccessResource = new PlainAccessResource();
        painAccessResource.setAccessKey("RocketMQ");
        painAccessResource.setSecretKey("12345678");
        painAccessResource.setWhiteRemoteAddress("127.0." + perm + ".*");
        painAccessResource.setDefaultGroupPerm(perm);
        painAccessResource.setDefaultTopicPerm(perm);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupA"), Permission.PUB);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupB"), Permission.SUB);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupC"), Permission.ANY);
        painAccessResource.addResourceAndPerm(PlainAccessResource.getRetryTopic("groupD"), Permission.DENY);

        painAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        painAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        painAccessResource.addResourceAndPerm("topicC", Permission.ANY);
        painAccessResource.addResourceAndPerm("topicD", Permission.DENY);
        return painAccessResource;
    }

    @Test
    public void buildPlainAccessResourceTest() {
        PlainAccessResource plainAccessResource = null;
        PlainAccessConfig plainAccess = new PlainAccessConfig();

        plainAccess.setAccessKey("RocketMQ");
        plainAccess.setSecretKey("12345678");
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.getAccessKey(), "RocketMQ");
        Assert.assertEquals(plainAccessResource.getSecretKey(), "12345678");

        plainAccess.setWhiteRemoteAddress("127.0.0.1");
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.getWhiteRemoteAddress(), "127.0.0.1");

        plainAccess.setAdmin(true);
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Assert.assertEquals(plainAccessResource.isAdmin(), true);

        List<String> groups = new ArrayList<String>();
        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        plainAccess.setGroupPerms(groups);
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 3);

        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupA")).byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupB")).byteValue(), Permission.PUB | Permission.SUB);
        Assert.assertEquals(resourcePermMap.get(PlainAccessResource.getRetryTopic("groupC")).byteValue(), Permission.PUB);

        List<String> topics = new ArrayList<String>();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");
        plainAccess.setTopicPerms(topics);
        plainAccessResource = plainPermissionManager.buildPlainAccessResource(plainAccess);
        resourcePermMap = plainAccessResource.getResourcePermMap();
        Assert.assertEquals(resourcePermMap.size(), 6);

        Assert.assertEquals(resourcePermMap.get("topicA").byteValue(), Permission.DENY);
        Assert.assertEquals(resourcePermMap.get("topicB").byteValue(), Permission.PUB | Permission.SUB);
        Assert.assertEquals(resourcePermMap.get("topicC").byteValue(), Permission.PUB);
    }

    @Test(expected = AclException.class)
    public void checkPermAdmin() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setRequestCode(17);
        plainPermissionManager.checkPerm(plainAccessResource, PUBPlainAccessResource);
    }

    @Test
    public void checkPerm() {

        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        plainPermissionManager.checkPerm(plainAccessResource, PUBPlainAccessResource);
        plainAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        plainPermissionManager.checkPerm(plainAccessResource, ANYPlainAccessResource);

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicB", Permission.SUB);
        plainPermissionManager.checkPerm(plainAccessResource, SUBPlainAccessResource);
        plainAccessResource.addResourceAndPerm("topicA", Permission.PUB);
        plainPermissionManager.checkPerm(plainAccessResource, ANYPlainAccessResource);

    }

    @Test(expected = AclException.class)
    public void checkErrorPermDefaultValueNotMatch() {

        plainAccessResource = new PlainAccessResource();
        plainAccessResource.addResourceAndPerm("topicF", Permission.PUB);
        plainPermissionManager.checkPerm(plainAccessResource, SUBPlainAccessResource);
    }

    @Test(expected = AclException.class)
    public void accountNullTest() {
        plainAccessConfig.setAccessKey(null);
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void accountThanTest() {
        plainAccessConfig.setAccessKey("123");
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void passWordtNullTest() {
        plainAccessConfig.setAccessKey(null);
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @Test(expected = AclException.class)
    public void passWordThanTest() {
        plainAccessConfig.setAccessKey("123");
        plainPermissionManager.buildPlainAccessResource(plainAccessConfig);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void cleanAuthenticationInfoTest() throws IllegalAccessException {
        // PlainPermissionManager.addPlainAccessResource(plainAccessResource);
        Map<String, List<PlainAccessResource>> plainAccessResourceMap = (Map<String, List<PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionManager, "plainAccessResourceMap", true);
        Assert.assertFalse(plainAccessResourceMap.isEmpty());

        plainPermissionManager.clearPermissionInfo();
        plainAccessResourceMap = (Map<String, List<PlainAccessResource>>) FieldUtils.readDeclaredField(plainPermissionManager, "plainAccessResourceMap", true);
        Assert.assertTrue(plainAccessResourceMap.isEmpty());
    }

    @Test
    public void testWatch() throws Exception {
        String dirPath = System.getProperty("rocketmq.home.dir") + System.getProperty("rocketmq.acl.plain.dir");
        assertThat(plainPermissionManager.isWatchStart()).isTrue();
        File watchFileAdd = new File(dirPath + File.separator + "plain_acl_watch_add.yml");
        try {
            //test add new plain acl file
            try (FileWriter fw = new FileWriter(watchFileAdd)) {
                fw.write("accounts:\r\n");
                fw.write("- accessKey: new-rocketmq-ak\r\n");
                fw.write("  secretKey: new-rocketmq-sk\r\n");
                fw.write("  whiteRemoteAddress: 127.0.0.2\r\n");
                fw.write("  admin: false\r\n");
                fw.flush();
            }

            Thread.sleep(1000L);
            Field field = PlainPermissionManager.class.getDeclaredField("plainAccessResourceMap");
            field.setAccessible(true);
            Map<String, PlainAccessResource> plainAccessResourceMap = (Map<String, PlainAccessResource>) field.get(plainPermissionManager);
            assertThat(plainAccessResourceMap.entrySet().size()).isEqualTo(3);
            PlainAccessResource par = plainAccessResourceMap.get("new-rocketmq-ak");
            assertThat(par.getSecretKey()).isEqualTo("new-rocketmq-sk");
            assertThat(par.getWhiteRemoteAddress()).isEqualTo("127.0.0.2");
            assertThat(par.isAdmin()).isFalse();

            //test update plain acl file
            Map<String, Object> updatedMap = AclUtils.getYamlDataObject(watchFileAdd.getAbsolutePath(), Map.class);
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) updatedMap.get("accounts");
            accounts.get(0).remove("accessKey");
            accounts.get(0).remove("secretKey");
            accounts.get(0).put("accessKey", "update-ak");
            accounts.get(0).put("secretKey", "update-sk");
            accounts.get(0).put("admin", "true");
            // Update file and flush to yaml file
            AclUtils.writeDataObject(watchFileAdd.getAbsolutePath(), updatedMap);

            Thread.sleep(1000);
            plainAccessResourceMap = (Map<String, PlainAccessResource>) field.get(plainPermissionManager);
            assertThat(plainAccessResourceMap.size()).isEqualTo(3);
            PlainAccessResource updatePAR = plainAccessResourceMap.get("update-ak");
            assertThat(updatePAR).isNotNull();
            assertThat(updatePAR.getSecretKey()).isEqualTo("update-sk");
            assertThat(updatePAR.getWhiteRemoteAddress()).isEqualTo("127.0.0.2");
            assertThat(updatePAR.isAdmin()).isTrue();
        } finally {
            watchFileAdd.delete();
        }

    }

    @Test
    public void testUpdateAccessConfig() throws Exception {
        String aclFileBakDirPath = System.getProperty("rocketmq.home.dir") + File.separator + "conf" + File.separator + "plain_acl_bak";
        String rocketmq2AclFilePath = System.getProperty("rocketmq.home.dir") + File.separator + System.getProperty("rocketmq.acl.plain.dir") + File.separator + "plain_acl_rocketmq2.yml";
        File defaultAclFile = new File(DEFAULT_PLAIN_ACL_FILE_PATH);
        File defaultBakAclFile = new File(aclFileBakDirPath + File.separator + "plain_acl_default_bak");
        File rocketmq2AclFile = new File(rocketmq2AclFilePath);
        File rocketmq2BakAclFile = new File(aclFileBakDirPath + File.separator + "plain_acl_rocketmq2_bak");
        Files.copy(defaultAclFile.toPath(), defaultBakAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(rocketmq2AclFile.toPath(), rocketmq2BakAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        try {
            //a new account
            String ak = "ak-" + System.currentTimeMillis();
            String sk = "sk-" + System.currentTimeMillis();
            PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
            plainAccessConfig.setAccessKey(ak);
            plainAccessConfig.setSecretKey(sk);
            plainAccessConfig.setGroupPerms(Lists.newArrayList("groupA=DENY", "groupB=SUB"));
            plainAccessConfig.setTopicPerms(Lists.newArrayList("topicA=PUB|SUB", "topicB=SUB"));
            plainPermissionManager.updateAccessConfig(plainAccessConfig);
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(DEFAULT_PLAIN_ACL_FILE_PATH, JSONObject.class);
            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            boolean writeToFileOk = false;
            for (PlainAccessConfig accessConfig : accounts.toJavaList(PlainAccessConfig.class)) {
                if (accessConfig.getAccessKey().equals(ak)) {
                    assertThat(accessConfig.getSecretKey()).isEqualTo(sk);
                    assertThat(accessConfig.getGroupPerms()).containsExactlyInAnyOrder("groupA=DENY", "groupB=SUB");
                    assertThat(accessConfig.getTopicPerms()).containsExactlyInAnyOrder("topicA=PUB|SUB", "topicB=SUB");
                    writeToFileOk = true;
                }
            }
            assertThat(writeToFileOk).isTrue();

            //no default plain acl file
            defaultAclFile.delete();
            plainPermissionManager.updateAccessConfig(plainAccessConfig);
            plainAclConfData = AclUtils.getYamlDataObject(DEFAULT_PLAIN_ACL_FILE_PATH, JSONObject.class);
            accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            writeToFileOk = false;
            for (PlainAccessConfig accessConfig : accounts.toJavaList(PlainAccessConfig.class)) {
                if (accessConfig.getAccessKey().equals(ak)) {
                    assertThat(accessConfig.getSecretKey()).isEqualTo(sk);
                    assertThat(accessConfig.getGroupPerms()).containsExactlyInAnyOrder("groupA=DENY", "groupB=SUB");
                    assertThat(accessConfig.getTopicPerms()).containsExactlyInAnyOrder("topicA=PUB|SUB", "topicB=SUB");
                    writeToFileOk = true;
                }
            }
            assertThat(writeToFileOk).isTrue();

            //old account existed in other acl file
            plainAccessConfig = new PlainAccessConfig();
            plainAccessConfig.setAccessKey("rocketmq2_acl_ak");
            plainAccessConfig.setSecretKey(sk);
            plainAccessConfig.setGroupPerms(Lists.newArrayList("groupA=DENY", "groupB=SUB"));
            plainAccessConfig.setTopicPerms(Lists.newArrayList("topicA=PUB|SUB", "topicB=SUB"));
            plainPermissionManager.updateAccessConfig(plainAccessConfig);
            plainAclConfData = AclUtils.getYamlDataObject(rocketmq2AclFilePath, JSONObject.class);
            accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            writeToFileOk = false;
            for (PlainAccessConfig accessConfig : accounts.toJavaList(PlainAccessConfig.class)) {
                if (accessConfig.getAccessKey().equals("rocketmq2_acl_ak")) {
                    assertThat(accessConfig.getSecretKey()).isEqualTo(sk);
                    assertThat(accessConfig.getGroupPerms()).containsExactlyInAnyOrder("groupA=DENY", "groupB=SUB");
                    assertThat(accessConfig.getTopicPerms()).containsExactlyInAnyOrder("topicA=PUB|SUB", "topicB=SUB");
                    writeToFileOk = true;
                }
            }
            assertThat(writeToFileOk).isTrue();
        } finally {
            Files.move(defaultBakAclFile.toPath(), defaultAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.move(rocketmq2BakAclFile.toPath(), rocketmq2AclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Test
    public void testDeleteAccessConfig() throws Exception {
        File defaultAclFile = new File(DEFAULT_PLAIN_ACL_FILE_PATH);
        String aclFileBakDirPath = System.getProperty("rocketmq.home.dir") + File.separator + "conf" + File.separator + "plain_acl_bak";
        File defaultBakAclFile = new File(aclFileBakDirPath + File.separator + "plain_acl_default_bak");
        Files.copy(defaultAclFile.toPath(), defaultBakAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        try {
            String ak = "RocketMQ";
            boolean result = plainPermissionManager.deleteAccessConfig(ak);
            assertThat(result).isTrue();
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(DEFAULT_PLAIN_ACL_FILE_PATH, JSONObject.class);
            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            assertThat(accounts.toJavaList(PlainAccessConfig.class)).isEmpty();
        } finally {
            Files.move(defaultBakAclFile.toPath(), defaultAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Test
    public void testUpdateGlobalWhiteAddrsConfig() throws Exception {
        File defaultAclFile = new File(DEFAULT_PLAIN_ACL_FILE_PATH);
        String aclFileBakDirPath = System.getProperty("rocketmq.home.dir") + File.separator + "conf" + File.separator + "plain_acl_bak";
        File defaultBakAclFile = new File(aclFileBakDirPath + File.separator + "plain_acl_default_bak");
        Files.copy(defaultAclFile.toPath(), defaultBakAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        try {
            List<String> globalWhiteAddrsList = Lists.newArrayList("2.2.2.2", "1.2.3.*");
            boolean result = plainPermissionManager.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList);
            assertThat(result).isTrue();
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(DEFAULT_PLAIN_ACL_FILE_PATH, JSONObject.class);
            JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
            assertThat(globalWhiteRemoteAddressesList.toJavaList(String.class)).containsExactlyInAnyOrder("2.2.2.2", "1.2.3.*");
        } finally {
            Files.move(defaultBakAclFile.toPath(), defaultAclFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Test
    public void testGetAllAclConfig() {
        AclConfig aclConfig = plainPermissionManager.getAllAclConfig();
        assertThat(aclConfig.getPlainAccessConfigs().size()).isEqualTo(2);
        assertThat(aclConfig.getGlobalWhiteAddrs()).containsExactlyInAnyOrder("1.1.1.1", "1.1.1.2", "1.1.2.*");
    }

    @Test
    public void testGetAclConfigDataVersion() {
        Map<String, DataVersion> versionMap = plainPermissionManager.getAclConfigDataVersion();
        assertThat(versionMap.size()).isEqualTo(2);
        for (Map.Entry<String, DataVersion> entry : versionMap.entrySet()) {
            String fileName = entry.getKey();
            DataVersion dataVersion = entry.getValue();
            switch (fileName) {
                case "plain_acl_default.yml":
                    assertThat(dataVersion.getCounter().get()).isEqualTo(1L);
                    assertThat(dataVersion.getTimestamp()).isEqualTo(1592825106625L);
                    break;
                case "plain_acl_rocketmq2.yml":
                    assertThat(dataVersion.getCounter().get()).isEqualTo(2L);
                    assertThat(dataVersion.getTimestamp()).isEqualTo(1592825100000L);
                    break;
            }
        }
    }
}
