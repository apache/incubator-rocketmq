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

/**
 * $Id: TopAddressingImpl.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.utils.HttpTinyClient;

public class TopAddressingImpl implements TopAddressing {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String nsAddr;
    private String wsAddr;
    private String unitName;
    private TopAddressing customTopAddressing;

    public TopAddressingImpl(final String wsAddr) {
        this(wsAddr, null);
    }

    public TopAddressingImpl(final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.customTopAddressing = findCustomTopAddressing();
    }

    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }

    private TopAddressing findCustomTopAddressing() {
        ServiceLoader<TopAddressing> serviceLoader = ServiceLoader.load(TopAddressing.class);
        Iterator<TopAddressing> iterator = serviceLoader.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public final String fetchNSAddr() {
        if (customTopAddressing != null) {
            return customTopAddressing.fetchNSAddr();
        }
        return fetchNSAddr(true, 3000);
    }

    @Override
    public void registerChangeCallBack(Consumer<String> changeCallBack) {
        if (customTopAddressing != null) {
            customTopAddressing.registerChangeCallBack(changeCallBack);
        }
    }

    public final String fetchNSAddr(boolean verbose, long timeoutMills) {
        String url = this.wsAddr;
        try {
            if (!UtilAll.isBlank(this.unitName)) {
                url = url + "-" + this.unitName + "?nofix=1";
            }
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(url, null, null, "UTF-8", timeoutMills);
            if (200 == result.code) {
                String responseStr = result.content;
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                } else {
                    log.error("fetch nameserver address is null");
                }
            } else {
                log.error("fetch nameserver address failed. statusCode=" + result.code);
            }
        } catch (IOException e) {
            if (verbose) {
                log.error("fetch name server address exception", e);
            }
        }

        if (verbose) {
            String errorMsg =
                "connect to " + url + " failed, maybe the domain name " + MixAll.getWSAddr() + " not bind in /etc/hosts";
            errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);

            log.warn(errorMsg);
        }
        return null;
    }

    public String getNsAddr() {
        return nsAddr;
    }

    public void setNsAddr(String nsAddr) {
        this.nsAddr = nsAddr;
    }
}
