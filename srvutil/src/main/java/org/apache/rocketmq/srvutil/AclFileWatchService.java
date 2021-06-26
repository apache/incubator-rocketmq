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

package org.apache.rocketmq.srvutil;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

public class AclFileWatchService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final String aclPath;
    private int aclFilesNum;
    private final Map<String, String> fileCurrentHash;
    private final AclFileWatchService.Listener listener;
    private static final int WATCH_INTERVAL = 500;
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public AclFileWatchService(String path, final AclFileWatchService.Listener listener) throws Exception {
        this.aclPath = path;
        this.fileCurrentHash = new HashMap<>();
        this.listener = listener;

        File aclDir = new File(path);
        String[] aclFileNames = aclDir.list();
        this.aclFilesNum = aclFileNames.length;
        for (int i = 0; i < aclFilesNum; i++) {
            String aclFilePath = this.aclPath + aclFileNames[i];
            if (StringUtils.isNotEmpty(aclFileNames[i]) && new File(aclFilePath).exists()) {
                this.fileCurrentHash.put(aclFilePath, hash(aclFilePath));
            }
        }

    }

    @Override
    public String getServiceName() {
        return "AclFileWatchService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(WATCH_INTERVAL);

                File aclDir = new File(aclPath);
                File[] aclFiles = aclDir.listFiles();
                int realAclFilesNum = aclFiles.length;

                if (aclFilesNum != realAclFilesNum) {
                    log.info("aclFilesNum: " + aclFilesNum + "  realAclFilesNum: " + realAclFilesNum);
                    aclFilesNum = realAclFilesNum;
                    log.info("aclFilesNum: " + aclFilesNum + "  realAclFilesNum: " + realAclFilesNum);
                    listener.onFileNumChanged(aclPath);
                } else {
                    for (int i = 0; i < aclFilesNum; i++) {
                        String fileName = aclFiles[i].getAbsolutePath();
                        String newHash = hash(fileName);
                        if (!newHash.equals(fileCurrentHash.get(i))) {
                            fileCurrentHash.put(fileName, newHash);
                            listener.onFileChanged(fileName);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    private String hash(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        md.update(Files.readAllBytes(path));
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target file is changed
         * @param aclFileName the changed file absolute path
         */
        void onFileChanged(String aclFileName);
        /**
         * Will be called when the number of the acl file is changed
         * @param path the path of the acl dir
         */
        void onFileNumChanged(String path);
    }
}
