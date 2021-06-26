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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.AclFileWatchService;

public class PlainPermissionManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
            System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String defaultAclDir = fileHome + File.separator
            + System.getProperty("rocketmq.acl.dir", "/conf/acl");

    private String defaultAclFile = fileHome + File.separator
            + System.getProperty("rocketmq.acl.dir", "/conf/acl") + File.separator + "plain_acl.yml";

    private  Map<String/** aclFileName **/, Map<String/** AccessKey **/, PlainAccessResource>> aclPlainAccessResourceMap = new HashMap<>();

    private Map<String/** AccessKey **/, String/** aclFileName **/> accessKeyTable = new HashMap<>();

    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private boolean isWatchStart;

    private Map<String/** aclFileName **/, DataVersion> dataVersionMap = new HashMap<>();

    public PlainPermissionManager() {
        load();
        watch();
    }

    public void load() {
        if (fileHome == null || fileHome.isEmpty()) {
            throw new AclException(String.format("%s file is empty", fileHome));
        }
        File aclDir = new File(defaultAclDir);
        File[] aclFiles = aclDir.listFiles();
        if (aclFiles == null || aclFiles.length == 0)
            return;
        if (aclPlainAccessResourceMap.size() != 0 && accessKeyTable.size() != 0) {
            aclPlainAccessResourceMap.clear();
            accessKeyTable.clear();
        }
        List<String> fileList = new ArrayList<>();
        for (File aclFile: aclFiles) {
            String aclFileAbsolutePath = aclFile.getAbsolutePath();
            load(aclFileAbsolutePath);
            fileList.add(aclFileAbsolutePath);
        }
        if (dataVersionMap.size() != aclFiles.length) {
            Iterator<Map.Entry<String, DataVersion>> it = dataVersionMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, DataVersion> entry = it.next();
                if (!fileList.contains(entry.getKey()))
                    it.remove();
            }
        }
    }

    public void load(String aclFilePath) {
        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

        JSONObject plainAclConfData = AclUtils.getYamlDataObject(aclFilePath,
                JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", aclFilePath));
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
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                this.accessKeyTable.put(plainAccessResource.getAccessKey(), aclFilePath);
            }
        }

        // For loading dataversion part just
        JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        DataVersion dataVersion = new DataVersion();
        if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
            List<DataVersion> dataVersions = tempDataVersion.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersions.get(0);
            dataVersion.assignNewOne(firstElement);
        }

        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.aclPlainAccessResourceMap.put(aclFilePath, plainAccessResourceMap);
        this.dataVersionMap.put(aclFilePath, dataVersion);
    }

    public Map<String, DataVersion> getAclConfigDataVersion() {
        return this.dataVersionMap;
    }

    public Map<String, Object> updateAclConfigFileVersion(Map<String, Object> updateAclConfigMap) {

        Object dataVersions = updateAclConfigMap.get(AclConstants.CONFIG_DATA_VERSION);
        DataVersion dataVersion = new DataVersion();
        List<Map<String, Object>> dataVersionList = new ArrayList<Map<String, Object>>();
        if (dataVersions != null) {
            dataVersionList = (List<Map<String, Object>>)dataVersions;
            dataVersion.setTimestamp((long)dataVersionList.get(0).get("timestamp"));
            dataVersion.setCounter(new AtomicLong(Long.parseLong(dataVersionList.get(0).get("counter").toString())));
        }
        dataVersion.nextVersion();
        List<Map<String, Object>> versionElement = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>();
        accountsMap.put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
        accountsMap.put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());

        versionElement.add(accountsMap);
        updateAclConfigMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);

        List<Map<String, Object>> accounts = (List<Map<String, Object>>)updateAclConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
        String accessKey = (String) accounts.get(0).get(AclConstants.CONFIG_ACCESS_KEY);
        String aclFileName = accessKeyTable.get(accessKey);
        dataVersionMap.put(aclFileName, dataVersion);
        return updateAclConfigMap;
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {

        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            throw new AclException("Parameter value plainAccessConfig is null, Please check your parameter");
        }

        Permission.checkResourcePerms(plainAccessConfig.getTopicPerms());
        Permission.checkResourcePerms(plainAccessConfig.getGroupPerms());

        if (accessKeyTable.containsKey(plainAccessConfig.getAccessKey())) {
            Map<String, Object> updateAccountMap = null;
            String aclFileName = accessKeyTable.get(plainAccessConfig.getAccessKey());
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            for (Map<String, Object> account : accounts) {
                if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                    // Update acl access config elements
                    accounts.remove(account);
                    updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                    accounts.add(updateAccountMap);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                    break;
                }
            }
            return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclAccessConfigMap));
        } else {
            //Create acl access config elements on the default acl file
            if (aclPlainAccessResourceMap.get(defaultAclFile) == null || aclPlainAccessResourceMap.get(defaultAclFile).size() == 0) {
                try {
                    File defaultAclFile = new File(fileHome + File.separator
                            + System.getProperty("rocketmq.acl.dir", "/conf/acl") + File.separator + "plain_acl.yml");
                    defaultAclFile.createNewFile();
                } catch (IOException e) {
                    log.warn("create default acl file has exception when update accessConfig. ", e);
                }
            }
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(defaultAclFile, Map.class);
            if (aclAccessConfigMap == null) {
                aclAccessConfigMap = new HashMap<>();
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, new ArrayList<>());
            }
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            return AclUtils.writeDataObject(defaultAclFile, updateAclConfigFileVersion(aclAccessConfigMap));
        }
    }

    public Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccountMap,
        PlainAccessConfig plainAccessConfig) {

        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccountMap;
        }

        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
            plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                "The accessKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey()));
        }
        newAccountsMap.put(AclConstants.CONFIG_ACCESS_KEY, plainAccessConfig.getAccessKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format(
                    "The secretKey=%s value length should longer than 6",
                    plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, (String) plainAccessConfig.getSecretKey());
        }
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
        if (plainAccessConfig.getTopicPerms() != null && !plainAccessConfig.getTopicPerms().isEmpty()) {
            newAccountsMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null && !plainAccessConfig.getGroupPerms().isEmpty()) {
            newAccountsMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
        }

        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accesskey) {
        if (StringUtils.isEmpty(accesskey)) {
            log.error("Parameter value accesskey is null or empty String,Please check your parameter");
            return false;
        }

        if (accessKeyTable.containsKey(accesskey)) {
            String aclFileName = accessKeyTable.get(accesskey);
            Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName,
                    Map.class);
            if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
                throw new AclException(String.format("the %s file is not found or empty", aclFileName));
            }
            List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
            Iterator<Map<String, Object>> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {
                if (itemIterator.next().get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
                    return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclAccessConfigMap));
                }
            }
        }
        return false;
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {

        if (globalWhiteAddrsList == null) {
            log.error("Parameter value globalWhiteAddrsList is null,Please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(defaultAclFile,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", defaultAclFile));
        }
        List<String> globalWhiteRemoteAddrList = (List<String>) aclAccessConfigMap.get(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);

        if (globalWhiteRemoteAddrList != null) {
            globalWhiteRemoteAddrList.clear();
            globalWhiteRemoteAddrList.addAll(globalWhiteAddrsList);

            // Update globalWhiteRemoteAddr element in memeory map firstly
            aclAccessConfigMap.put(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS, globalWhiteRemoteAddrList);
            return AclUtils.writeDataObject(defaultAclFile, updateAclConfigFileVersion(aclAccessConfigMap));
        }

        log.error("Users must ensure that the acl yaml config file has globalWhiteRemoteAddresses flag in the %s firstly", defaultAclFile);
        return false;
    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();

        File aclDir = new File(defaultAclDir);
        File[] aclFileNames = aclDir.listFiles();
        for (File file : aclFileNames) {
            String path = file.getAbsolutePath();
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(path,
                    JSONObject.class);
            if (plainAclConfData == null || plainAclConfData.isEmpty()) {
                throw new AclException(String.format("%s file is not data", path));
            }
            JSONArray globalWhiteAddrs = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
            if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
                whiteAddrs = globalWhiteAddrs.toJavaList(String.class);
            }
            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            if (accounts != null && !accounts.isEmpty()) {
                configs.addAll(accounts.toJavaList(PlainAccessConfig.class));
            }
            aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        }
        aclConfig.setPlainAccessConfigs(configs);
        return aclConfig;
    }

    private void watch() {
        try {
            AclFileWatchService aclFileWatchService = new AclFileWatchService(defaultAclDir, new AclFileWatchService.Listener() {
                @Override
                public void onFileChanged(String aclFileName) {
                    load(aclFileName);
                }

                @Override
                public void onFileNumChanged(String path) {
                    load();
                }
            });
            aclFileWatchService.start();
            log.info("Succeed to start AclFileWatchService");
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
        this.aclPlainAccessResourceMap.clear();
        this.accessKeyTable.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
            || plainAccessConfig.getSecretKey() == null
            || plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH
            || plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
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


        if (!accessKeyTable.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accesskey
        String aclFileName = accessKeyTable.get(plainAccessResource.getAccessKey());
        PlainAccessResource ownedAccess = aclPlainAccessResourceMap.get(aclFileName).get(plainAccessResource.getAccessKey());
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
