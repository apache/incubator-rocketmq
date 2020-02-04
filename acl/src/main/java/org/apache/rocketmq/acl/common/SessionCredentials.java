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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.rocketmq.common.MixAll;

public class SessionCredentials {
    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final String ACCESS_KEY = "AccessKey";
    public static final String SECRET_KEY = "SecretKey";
    public static final String SIGNATURE = "Signature";
    public static final String SECURITY_TOKEN = "SecurityToken";

    public static final String KEY_FILE = System.getProperty("rocketmq.client.keyFile",
        System.getProperty("user.home") + File.separator + "key");

    private String accessKey;
    private String secretKey;
    private String securityToken;
    private String signature;

    public SessionCredentials() {
        String keyContent = null;
        try {
            keyContent = MixAll.file2String(KEY_FILE);
        } catch (IOException ignore) {
        }
        if (keyContent != null) {
            Properties prop = MixAll.string2Properties(keyContent);
            if (prop != null) {
                this.updateContent(prop);
            }
        }
    }

    public SessionCredentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public SessionCredentials(String accessKey, String secretKey, String securityToken) {
        this(accessKey, secretKey);
        this.securityToken = securityToken;
    }

    public void updateContent(Properties prop) {
        {
            String value = prop.getProperty(ACCESS_KEY);
            if (value != null) {
                this.accessKey = value.trim();
            }
        }
        {
            String value = prop.getProperty(SECRET_KEY);
            if (value != null) {
                this.secretKey = value.trim();
            }
        }
        {
            String value = prop.getProperty(SECURITY_TOKEN);
            if (value != null) {
                this.securityToken = value.trim();
            }
        }
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public void setSecurityToken(final String securityToken) {
        this.securityToken = securityToken;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((accessKey == null) ? 0 : accessKey.hashCode());
        result = prime * result + ((secretKey == null) ? 0 : secretKey.hashCode());
        result = prime * result + ((signature == null) ? 0 : signature.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        SessionCredentials other = (SessionCredentials) obj;
        if (accessKey == null) {
            if (other.accessKey != null)
                return false;
        } else if (!accessKey.equals(other.accessKey))
            return false;

        if (secretKey == null) {
            if (other.secretKey != null)
                return false;
        } else if (!secretKey.equals(other.secretKey))
            return false;

        if (signature == null) {
            if (other.signature != null)
                return false;
        } else if (!signature.equals(other.signature))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "SessionCredentials [accessKey=" + accessKey + ", secretKey=" + secretKey + ", signature="
            + signature + ", SecurityToken=" + securityToken + "]";
    }
}