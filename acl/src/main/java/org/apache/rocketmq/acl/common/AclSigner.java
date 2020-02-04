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

import java.nio.charset.Charset;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class AclSigner {
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    public static final SigningAlgorithm DEFAULT_ALGORITHM = SigningAlgorithm.HmacSHA1;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_AUTHORIZE_LOGGER_NAME);
    private static final int CAL_SIGNATURE_FAILED = 10015;
    private static final String CAL_SIGNATURE_FAILED_MSG = "[%s:signature-failed] unable to calculate a request signature. error=%s";

    public static String calSignature(String data, String key) throws AclException {
        return calSignature(data, key, DEFAULT_ALGORITHM, DEFAULT_CHARSET);
    }

    public static String calSignature(String data, String key, SigningAlgorithm algorithm,
        Charset charset) throws AclException {
        return signAndBase64Encode(data, key, algorithm, charset);
    }

    private static String signAndBase64Encode(String data, String key, SigningAlgorithm algorithm, Charset charset)
        throws AclException {
        try {
            byte[] signature = sign(data.getBytes(charset), key.getBytes(charset), algorithm);
            return new String(Base64.encodeBase64(signature), DEFAULT_CHARSET);
        } catch (Exception e) {
            String message = String.format(CAL_SIGNATURE_FAILED_MSG, CAL_SIGNATURE_FAILED, e.getMessage());
            log.error(message, e);
            throw new AclException("CAL_SIGNATURE_FAILED", CAL_SIGNATURE_FAILED, message, e);
        }
    }

    private static byte[] sign(byte[] data, byte[] key, SigningAlgorithm algorithm) throws AclException {
        try {
            Mac mac = Mac.getInstance(algorithm.toString());
            mac.init(new SecretKeySpec(key, algorithm.toString()));
            return mac.doFinal(data);
        } catch (Exception e) {
            String message = String.format(CAL_SIGNATURE_FAILED_MSG, CAL_SIGNATURE_FAILED, e.getMessage());
            log.error(message, e);
            throw new AclException("CAL_SIGNATURE_FAILED", CAL_SIGNATURE_FAILED, message, e);
        }
    }

    public static String calSignature(byte[] data, String key) throws AclException {
        return calSignature(data, key, DEFAULT_ALGORITHM, DEFAULT_CHARSET);
    }

    public static String calSignature(byte[] data, String key, SigningAlgorithm algorithm,
        Charset charset) throws AclException {
        return signAndBase64Encode(data, key, algorithm, charset);
    }

    private static String signAndBase64Encode(byte[] data, String key, SigningAlgorithm algorithm, Charset charset)
        throws AclException {
        try {
            byte[] signature = sign(data, key.getBytes(charset), algorithm);
            return new String(Base64.encodeBase64(signature), DEFAULT_CHARSET);
        } catch (Exception e) {
            String message = String.format(CAL_SIGNATURE_FAILED_MSG, CAL_SIGNATURE_FAILED, e.getMessage());
            log.error(message, e);
            throw new AclException("CAL_SIGNATURE_FAILED", CAL_SIGNATURE_FAILED, message, e);
        }
    }

}
