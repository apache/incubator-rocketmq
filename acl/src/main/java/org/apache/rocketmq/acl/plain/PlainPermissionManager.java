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
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class PlainPermissionManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final String FILE_HOME = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private static final String DEFAULT_PLAIN_ACL_FILE_DIR = System.getProperty("rocketmq.acl.plain.dir", "/conf/plain_acl");
    private static final String DEFAULT_PLAIN_ACL_FILE_DIR_PATH = FILE_HOME + File.separator + DEFAULT_PLAIN_ACL_FILE_DIR;
    private static final String DEFAULT_PLAIN_ACL_FILE = "plain_acl_default.yml";
    private static final String DEFAULT_PLAIN_ACL_FILE_PATH = FILE_HOME + File.separator + DEFAULT_PLAIN_ACL_FILE_DIR + File.separator + DEFAULT_PLAIN_ACL_FILE;


    private Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
    private Map<String, String> akFilepathMap = new HashMap<>();
    private Map<String, RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new HashMap<>();

    private final RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private boolean isWatchStart;

    private final DataVersion dataVersion = new DataVersion();

    public PlainPermissionManager() {
        load();
        watch();
    }

    public void load() {
        try {
            File aclFileDir = new File(FILE_HOME + File.separator + DEFAULT_PLAIN_ACL_FILE_DIR);
            File[] aclFiles = aclFileDir.listFiles();
            if (aclFiles == null) {
                return;
            }
            Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
            Map<String, RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new HashMap<>();
            Map<String, String> akFilepathMap = new HashMap<>();

            for (File aclFile : aclFiles) {
                load(aclFile.getAbsolutePath(), plainAccessResourceMap, globalWhiteRemoteAddressStrategy, akFilepathMap);
            }
            this.plainAccessResourceMap = plainAccessResourceMap;
            this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
            this.akFilepathMap = akFilepathMap;
        } catch (Exception e) {
            log.error("Load files error:{}", e);
        }
    }

    private void load(String filepath, Map<String, PlainAccessResource> plainAccessResourceMap,
                      Map<String, RemoteAddressStrategy> globalWhiteRemoteAddressStrategy, Map<String, String> akFilepathMap) {

        JSONObject plainAclConfData = AclUtils.getYamlDataObject(filepath, JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            log.warn("{} file is empty", filepath);
            return;
        }
        log.info("File:{}, Broker plain acl conf data is : {}", new File(filepath).getName(), plainAclConfData.toString());
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                String remoteAddress = globalWhiteRemoteAddressesList.getString(i);
                globalWhiteRemoteAddressStrategy.putIfAbsent(remoteAddress, remoteAddressStrategyFactory.getRemoteAddressStrategy(remoteAddress));
            }
        }

        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                akFilepathMap.put(plainAccessResource.getAccessKey(), filepath);
            }
        }

        JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
            List<DataVersion> dataVersion = tempDataVersion.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersion.get(0);
            this.dataVersion.assignNewOne(firstElement);
        }
    }

    public Map<String, DataVersion> getAclConfigDataVersion() {
        File[] files = new File(DEFAULT_PLAIN_ACL_FILE_DIR_PATH).listFiles();
        if (files == null) {
            return Collections.emptyMap();
        }
        Map<String, DataVersion> versionMap = new HashMap<>();
        for (File file : files) {
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(file.getAbsolutePath(), JSONObject.class);
            JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
            if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
                List<DataVersion> dataVersion = tempDataVersion.toJavaList(DataVersion.class);
                versionMap.putIfAbsent(file.getName(), dataVersion.get(0));
            }
        }
        return versionMap;
    }

    private Map<String, Object> updateAclConfigFileVersion(Map<String, Object> updateAclConfigMap) {

        dataVersion.nextVersion();
        List<Map<String, Object>> versionElement = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>() {
            {
                put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
                put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());
            }
        };
        versionElement.add(accountsMap);
        updateAclConfigMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);
        return updateAclConfigMap;
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            return false;
        }

        String filepath = akFilepathMap.get(plainAccessConfig.getAccessKey());
        if (filepath == null) {
            //new account use default acl file
            filepath = DEFAULT_PLAIN_ACL_FILE_PATH;
            File defaultFile = new File(filepath);
            if (!defaultFile.exists()) {
                try {
                    defaultFile.createNewFile();
                } catch (IOException e) {
                    log.error("Create default acl file");
                }
            }
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(filepath, Map.class);
        if (aclAccessConfigMap == null) {
            aclAccessConfigMap = new HashMap<>();
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, new ArrayList<>());
        }

        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> updateAccountMap;
        for (Map<String, Object> account : accounts) {
            if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                // Update acl access config elements
                accounts.remove(account);
                updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                return AclUtils.writeDataObject(filepath, updateAclConfigFileVersion(aclAccessConfigMap));
            }
        }
        // Create acl access config elements
        accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
        aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
        return AclUtils.writeDataObject(filepath, updateAclConfigFileVersion(aclAccessConfigMap));
    }

    private Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccountMap, PlainAccessConfig plainAccessConfig) {
        
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
            newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
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
        String filepath = akFilepathMap.get(accesskey);
        if (filepath == null) {
            throw new AclException(String.format("Accesskey: %s not existed, Please check it again", accesskey));
        }
        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(filepath, Map.class);

        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
        if (accounts != null) {
            Iterator<Map<String, Object>> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {

                if (itemIterator.next().get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    return AclUtils.writeDataObject(filepath, updateAclConfigFileVersion(aclAccessConfigMap));
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

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(DEFAULT_PLAIN_ACL_FILE_PATH, Map.class);
        List<String> globalWhiteRemoteAddrList = (List<String>) aclAccessConfigMap.get(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);

        if (globalWhiteRemoteAddrList != null) {
            globalWhiteRemoteAddrList.clear();
            globalWhiteRemoteAddrList.addAll(globalWhiteAddrsList);

            // Update globalWhiteRemoteAddr element in memeory map firstly
            aclAccessConfigMap.put(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS, globalWhiteRemoteAddrList);
            return AclUtils.writeDataObject(DEFAULT_PLAIN_ACL_FILE_PATH, updateAclConfigFileVersion(aclAccessConfigMap));
        }
        return false;
    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        File[] files = new File(DEFAULT_PLAIN_ACL_FILE_DIR_PATH).listFiles();
        if (files == null) {
            return aclConfig;
        }
        for (File file : files) {
            JSONObject plainAclConfData = AclUtils.getYamlDataObject(file.getAbsolutePath(), JSONObject.class);
            if (plainAclConfData == null || plainAclConfData.isEmpty()) {
                continue;
            }
            JSONArray globalWhiteAddrs = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
            if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
                whiteAddrs.addAll(globalWhiteAddrs.toJavaList(String.class));
            }
            JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
            if (accounts != null && !accounts.isEmpty()) {
                configs.addAll(accounts.toJavaList(PlainAccessConfig.class));
            }
        }
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        aclConfig.setPlainAccessConfigs(configs);
        return aclConfig;
    }

    private void watch() {
        String watchDirPath = FILE_HOME + DEFAULT_PLAIN_ACL_FILE_DIR;
        try {
            FileAlterationMonitor monitor = new FileAlterationMonitor(500L);
            FileAlterationObserver observer = new FileAlterationObserver(new File(watchDirPath));
            observer.addListener(new FileAlterationListenerAdaptor() {
                @Override
                public void onFileCreate(File file) {
                    super.onFileCreate(file);
                    log.info("The plain acl yml file:{} created, reload the context", file);
                    try {
                        load();
                    } catch (Exception e) {
                        log.error("Reload error:{}", e);
                    }
                }

                @Override
                public void onFileChange(File file) {
                    super.onFileChange(file);
                    log.info("The plain acl yml file:{} changed, reload the context", file);
                    try {
                        load();
                    } catch (Exception e) {
                        log.error("Reload error:{}", e);
                    }
                }

                @Override
                public void onFileDelete(File file) {
                    super.onFileDelete(file);
                    log.info("The plain acl yml file:{} deleted, reload the context", file);
                    try {
                        load();
                    } catch (Exception e) {
                        log.error("Reload error:{}", e);
                    }
                }
            });
            monitor.addObserver(observer);
            monitor.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    monitor.stop();
                    log.info("Shutdown acl file dir monitor success");
                } catch (Exception e) {
                    log.error("Shutdown acl file dir monitor error:{}", e);
                }
            }));
            log.info("Succeed to start monitor dir:{}", watchDirPath);
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start monitor dir:{}", watchDirPath);
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
        for (Map.Entry<String, RemoteAddressStrategy> entry : globalWhiteRemoteAddressStrategy.entrySet()) {
            // Check the global white remote addr
            if (entry.getValue().match(plainAccessResource)) {
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