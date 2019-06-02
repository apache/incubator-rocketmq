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
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.FileWatchService;

public class PlainPermissionLoader {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final String DEFAULT_PLAIN_ACL_FILE = "/conf/plain_acl.yml";

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String fileName = System.getProperty("rocketmq.acl.plain.file", DEFAULT_PLAIN_ACL_FILE);

    private  Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();

    private  List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private boolean isWatchStart;

    private final DataVersion dataVersion = new DataVersion();

    public PlainPermissionLoader() {
        load();
        watch();
    }

    public void load() {

        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file  is not data", fileHome + File.separator + fileName));
        }
        log.info("Broker plain acl conf data is : ", plainAclConfData.toString());
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                        getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(i)));
            }
        }

        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(),plainAccessResource);
            }
        }

        //for loading dataversion part just
        JSONArray dataVersion4Yaml = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        if (dataVersion4Yaml != null && !dataVersion4Yaml.isEmpty()) {
            List<DataVersion> dataVersion = dataVersion4Yaml.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersion.get(0);
            this.dataVersion.assignNewOne(firstElement);
        }

        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.plainAccessResourceMap = plainAccessResourceMap;
    }

    public String getAclConfigDataVersion() {
        return this.dataVersion.toJson();
    }

    public boolean createOrUpdateVersionInFile() {

        //for updating dataversion part
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);

        JSONArray dataVersion4Yaml = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        if (dataVersion4Yaml != null && !dataVersion4Yaml.isEmpty()) {
            log.info("the acl yaml config's content is changed,then change the dataVersion");
            List<DataVersion> dataVersion = dataVersion4Yaml.toJavaList(DataVersion.class);
            //dataversion is only one element in acl yam file
            DataVersion firstElement = dataVersion.get(0);
            firstElement.nextVersion();
            this.dataVersion.assignNewOne(firstElement);
            if (updateConfigFileVersion(false)) {
                log.info("update datavesion success!");
                return true;
            } else {
                log.error("update dataversion failed!");
                return false;
            }
        } else {
            log.info("the acl yaml config is starting to load,and dataVersion will be initialized");
            if (updateConfigFileVersion(true)) {
                log.info("initial datavesion success!");
                return true;
            } else {
                log.error("initial dataversion failed!");
                return false;
            }
        }
    }

    private boolean updateConfigFileVersion(boolean isNewOne) {

        Map<String, Object> originalMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);

        List<Map<String, Object>> versionElement = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>() {
            {
                put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
                put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());
            }
        };
        versionElement.add(accountsMap);
        originalMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);
        if (isNewOne) {
            return AclUtils.writeDataObject2Yaml(fileHome + File.separator + fileName, originalMap);
        } else {
            Map<String, Object> updatedDataVersionMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName, Map.class);
            List<Map<String, Object>> dataVersionList = (List<Map<String, Object>>) updatedDataVersionMap.get(AclConstants.CONFIG_DATA_VERSION);
            dataVersionList.clear();
            dataVersionList.add(accountsMap);
            return AclUtils.writeDataObject2Yaml(fileHome + File.separator + fileName, updatedDataVersionMap);
        }
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {

        if (plainAccessConfig == null) {
            log.error("parameter value plainAccessConfig is null,please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);

        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> updateAccountMap = null;
        if (accounts != null && !accounts.isEmpty()) {
            for (Map<String, Object> account : accounts) {
                if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                    //update acl access config elements
                    accounts.remove(account);
                    updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                    accounts.add(updateAccountMap);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    if (AclUtils.writeDataObject2Yaml(fileHome + File.separator + fileName, aclAccessConfigMap)) {
                        if (createOrUpdateVersionInFile()) {
                            return true;
                        }
                    }
                    return false;
                }
            }
            //create acl access config elements
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            if (AclUtils.writeDataObject2Yaml(fileHome + File.separator + fileName, aclAccessConfigMap)) {
                if (createOrUpdateVersionInFile()) {
                    return true;
                }
            }
            return false;
        }

        log.error("users must ensure the acl yaml config file has accounts node element");
        return false;
    }

    private Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccoutMap, PlainAccessConfig plainAccessConfig) {

        Map<String, Object> newAccountsMap = null;
        if (existedAccoutMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccoutMap;
        }

        if (plainAccessConfig.getAccessKey() == null
            || plainAccessConfig.getSecretKey() == null
            || plainAccessConfig.getAccessKey().length() <= 6
            || plainAccessConfig.getSecretKey().length() <= 6) {
            throw new AclException(String.format(
                "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
        
        newAccountsMap.put(AclConstants.CONFIG_ACCESS_KEY, plainAccessConfig.getAccessKey());
        newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, (String)plainAccessConfig.getSecretKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getWhiteRemoteAddress())) {
            newAccountsMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig.getDefaultGroupPerm());
        }
        if (!plainAccessConfig.getTopicPerms().isEmpty()) {
            newAccountsMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
        }
        if (!plainAccessConfig.getGroupPerms().isEmpty()) {
            newAccountsMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
        }

        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accesskey) {
        if (StringUtils.isEmpty(accesskey)) {
            log.error("parameter value accesskey is null or empty String,please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
                    Map.class);

        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
        if (accounts != null && !accounts.isEmpty()) {
            for (Map<String, Object> account : accounts) {
                if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    //delete the related acl config element
                    accounts.remove(account);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    if (AclUtils.writeDataObject2Yaml(fileHome + File.separator + fileName, aclAccessConfigMap)) {
                        if (createOrUpdateVersionInFile()) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        }
        log.error("users must ensure the acl yaml config file has related acl config elements");

        return false;
    }

    private void watch() {
        try {
            String watchFilePath = fileHome + fileName;
            FileWatchService fileWatchService = new FileWatchService(new String[] {watchFilePath}, new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    log.info("The plain acl yml changed, reload the context");
                    load();
                }
            });
            fileWatchService.start();
            log.info("Succeed to start AclWatcherService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        if (Permission.needAdminPerm(needCheckedAccess.getRequestCode()) && !ownedAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", needCheckedAccess.getRequestCode(), ownedAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = needCheckedAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedAccess.getResourcePermMap();

        if (needCheckedPermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }

        if (ownedPermMap == null && ownedAccess.isAdmin()) {
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);

            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // Check the default perm
                byte ownedPerm = isGroup ? ownedAccess.getDefaultGroupPerm() :
                    ownedAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }

    void clearPermissionInfo() {
        this.plainAccessResourceMap.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
            || plainAccessConfig.getSecretKey() == null
            || plainAccessConfig.getAccessKey().length() <= 6
            || plainAccessConfig.getSecretKey().length() <= 6) {
            throw new AclException(String.format(
                "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                    plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccessConfig.getAccessKey());
        plainAccessResource.setSecretKey(plainAccessConfig.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());

        plainAccessResource.setAdmin(plainAccessConfig.isAdmin());

        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultTopicPerm()));

        Permission.parseResourcePerms(plainAccessResource, false, plainAccessConfig.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccessConfig.getTopicPerms());

        plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategyFactory.
                getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress()));

        return plainAccessResource;
    }

    public void validate(PlainAccessResource plainAccessResource) {

        // Check the global white remote addr
        for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
            if (remoteAddressStrategy.match(plainAccessResource)) {
                return;
            }
        }

        if (plainAccessResource.getAccessKey() == null) {
            throw new AclException(String.format("No accessKey is configured"));
        }

        if (!plainAccessResourceMap.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accesskey
        PlainAccessResource ownedAccess = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }

        // Check the signature
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        // Check perm of each resource

        checkPerm(plainAccessResource, ownedAccess);
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }
}
