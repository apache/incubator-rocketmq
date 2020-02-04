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
package org.apache.rocketmq.acl.common;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.yaml.snakeyaml.Yaml;

import static org.apache.rocketmq.acl.common.SessionCredentials.CHARSET;

public class AclUtils {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static byte[] combineRequestContent(RemotingCommand request, SortedMap<String, String> fieldsMap) {
        try {
            StringBuilder sb = new StringBuilder("");
            for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                    sb.append(entry.getValue());
                }
            }

            return AclUtils.combineBytes(sb.toString().getBytes(CHARSET), request.getBody());
        } catch (Exception e) {
            throw new RuntimeException("Incompatible exception.", e);
        }
    }

    public static byte[] combineBytes(byte[] b1, byte[] b2) {
        int size = (null != b1 ? b1.length : 0) + (null != b2 ? b2.length : 0);
        byte[] total = new byte[size];
        if (null != b1)
            System.arraycopy(b1, 0, total, 0, b1.length);
        if (null != b2)
            System.arraycopy(b2, 0, total, b1.length, b2.length);
        return total;
    }

    public static String calSignature(byte[] data, String secretKey) {
        String signature = AclSigner.calSignature(data, secretKey);
        return signature;
    }

    public static void IPv6AddressCheck(String netaddress) {
        if (isAsterisk(netaddress) || isMinus(netaddress)) {
            int asterisk = netaddress.indexOf("*");
            int minus = netaddress.indexOf("-");
//            '*' must be the end of netaddress if it exists
            if (asterisk > -1 && asterisk != netaddress.length() - 1) {
                throw new AclException(String.format("Netaddress examine scope Exception netaddress is %s", netaddress));
            }

//            format like "2::ac5:78:1-200:*" or "2::ac5:78:1-200" is legal
            if (minus > -1) {
                if (asterisk == -1) {
                    if (minus <= netaddress.lastIndexOf(":")) {
                        throw new AclException(String.format("Netaddress examine scope Exception netaddress is %s", netaddress));
                    }
                } else {
                    if (minus <= netaddress.lastIndexOf(":", netaddress.lastIndexOf(":") - 1)) {
                        throw new AclException(String.format("Netaddress examine scope Exception netaddress is %s", netaddress));
                    }
                }
            }
        }
    }

    public static String v6ipProcess(String netaddress, String[] strArray, int index) {
        int part;
        String subAddress;
        boolean isAsterisk = isAsterisk(netaddress);
        boolean isMinus = isMinus(netaddress);
        if (isAsterisk && isMinus) {
            part = 6;
            int lastColon = netaddress.lastIndexOf(':');
            int secondLastColon = netaddress.substring(0, lastColon).lastIndexOf(':');
            subAddress = netaddress.substring(0, secondLastColon);
        } else if (!isAsterisk && !isMinus) {
            part = 8;
            subAddress = netaddress;
        } else {
            part = 7;
            subAddress = netaddress.substring(0, netaddress.lastIndexOf(':'));
        }
        return expandIP(subAddress, part);
    }

    public static void verify(String netaddress, int index) {
        if (!AclUtils.isScope(netaddress, index)) {
            throw new AclException(String.format("Netaddress examine scope Exception netaddress is %s", netaddress));
        }
    }

    public static String[] getAddreeStrArray(String netaddress, String partialAddress) {
        String[] parAddStrArray = StringUtils.split(partialAddress.substring(1, partialAddress.length() - 1), ",");
        String address = netaddress.substring(0, netaddress.indexOf("{"));
        String[] addreeStrArray = new String[parAddStrArray.length];
        for (int i = 0; i < parAddStrArray.length; i++) {
            addreeStrArray[i] = address + parAddStrArray[i];
        }
        return addreeStrArray;
    }

    public static boolean isScope(String netaddress, int index) {
//        IPv6 Address
        if (isColon(netaddress)) {
            netaddress = expandIP(netaddress, 8);
            String[] strArray = StringUtils.split(netaddress, ":");
            return isIPv6Scope(strArray, index);
        }

        String[] strArray = StringUtils.split(netaddress, ".");
        if (strArray.length != 4) {
            return false;
        }
        return isScope(strArray, index);

    }

    public static boolean isScope(String[] num, int index) {
        if (num.length <= index) {

        }
        for (int i = 0; i < index; i++) {
            if (!isScope(num[i])) {
                return false;
            }
        }
        return true;

    }

    public static boolean isColon(String netaddress) {
        return netaddress.indexOf(':') > -1;
    }

    public static boolean isScope(String num) {
        return isScope(Integer.valueOf(num.trim()));
    }

    public static boolean isScope(int num) {
        return num >= 0 && num <= 255;
    }

    public static boolean isAsterisk(String asterisk) {
        return asterisk.indexOf('*') > -1;
    }

    public static boolean isComma(String colon) {
        return colon.indexOf(',') > -1;
    }

    public static boolean isMinus(String minus) {
        return minus.indexOf('-') > -1;

    }

    public static boolean isIPv6Scope(String[] num, int index) {
        for (int i = 0; i < index; i++) {
            int value;
            try {
                value = Integer.parseInt(num[i], 16);
            } catch (NumberFormatException e) {
                return false;
            }
            if (!isIPv6Scope(value)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isIPv6Scope(int num) {
        int min = Integer.parseInt("0", 16);
        int max = Integer.parseInt("ffff", 16);
        return num >= min && num <= max;
    }

    public static String expandIP(String netaddress, int part) {
        boolean compress = false;
        int compressIndex = -1;
        String[] strArray = StringUtils.split(netaddress, ":");
        ArrayList<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < netaddress.length(); i++) {
            if (netaddress.charAt(i) == ':') {
                if (indexes.size() > 0 && i - indexes.get(indexes.size() - 1) == 1) {
                    compressIndex = i;
                    compress = true;
                }
                indexes.add(i);
            }
        }

        for (int i = 0; i < strArray.length; i++) {
            if (strArray[i].length() < 4) {
                strArray[i] = "0000".substring(0, 4 - strArray[i].length()) + strArray[i];
            }
        }

        StringBuilder sb = new StringBuilder();
        if (compress) {
            int pos = indexes.indexOf(compressIndex);
            int index = 0;
            if (!netaddress.startsWith(":")) {
                for (int i = 0; i < pos; i++) {
                    sb.append(strArray[index]).append(":");
                    index += 1;
                }
            }
            int zeroNum = part - strArray.length;
            if (netaddress.endsWith(":")) {
                for (int i = 0; i < zeroNum; i++) {
                    sb.append("0000");
                    if (i != zeroNum - 1) {
                        sb.append(":");
                    }
                }
            } else {
                for (int i = 0; i < zeroNum; i++) {
                    sb.append("0000").append(":");
                }
                for (int i = index; i < strArray.length; i++) {
                    sb.append(strArray[i]);
                    if (i != strArray.length - 1) {
                        sb.append(":");
                    }
                }
            }
        } else {
            for (int i = 0; i < strArray.length; i++) {
                sb.append(strArray[i]);
                if (i != strArray.length - 1) {
                    sb.append(":");
                }
            }
        }
        return sb.toString().toUpperCase();
    }

    public static <T> T getYamlDataObject(String path, Class<T> clazz) {
        Yaml yaml = new Yaml();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(path));
            return yaml.loadAs(fis, clazz);
        } catch (FileNotFoundException ignore) {
            return null;
        } catch (Exception e) {
            throw new AclException(e.getMessage());
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    public static boolean writeDataObject(String path, Map<String, Object> dataMap) {
        Yaml yaml = new Yaml();
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileWriter(path));
            String dumpAsMap = yaml.dumpAsMap(dataMap);
            pw.print(dumpAsMap);
            pw.flush();
        } catch (Exception e) {
            throw new AclException(e.getMessage());
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
        return true;
    }

    public static RPCHook getAclRPCHook(String fileName) {
        JSONObject yamlDataObject = null;
        try {
            yamlDataObject = AclUtils.getYamlDataObject(fileName,
                JSONObject.class);
        } catch (Exception e) {
            log.error("Convert yaml file to data object error, ", e);
            return null;
        }

        if (yamlDataObject == null || yamlDataObject.isEmpty()) {
            log.warn("Cannot find conf file :{}, acl isn't be enabled.", fileName);
            return null;
        }

        String accessKey = yamlDataObject.getString(AclConstants.CONFIG_ACCESS_KEY);
        String secretKey = yamlDataObject.getString(AclConstants.CONFIG_SECRET_KEY);

        if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
            log.warn("AccessKey or secretKey is blank, the acl is not enabled.");

            return null;
        }
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

}
