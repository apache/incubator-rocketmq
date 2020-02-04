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

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class RemoteAddressStrategyFactory {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static final NullRemoteAddressStrategy NULL_NET_ADDRESS_STRATEGY = new NullRemoteAddressStrategy();

    public static final BlankRemoteAddressStrategy BLANK_NET_ADDRESS_STRATEGY = new BlankRemoteAddressStrategy();

    public RemoteAddressStrategy getRemoteAddressStrategy(PlainAccessResource plainAccessResource) {
        return getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress());
    }

    public RemoteAddressStrategy getRemoteAddressStrategy(String remoteAddr) {
        if (StringUtils.isBlank(remoteAddr)) {
            return BLANK_NET_ADDRESS_STRATEGY;
        }
        if ("*".equals(remoteAddr) || "*.*.*.*".equals(remoteAddr) || "*:*:*:*:*:*:*:*".equals(remoteAddr)) {
            return NULL_NET_ADDRESS_STRATEGY;
        }
        if (remoteAddr.endsWith("}")) {
            if (AclUtils.isColon(remoteAddr)) {
                String[] strArray = StringUtils.split(remoteAddr, ":");
                String last = strArray[strArray.length - 1];
                if (!last.startsWith("{")) {
                    throw new AclException(String.format("MultipleRemoteAddressStrategy netaddress examine scope Exception netaddress", remoteAddr));
                }
                return new MultipleRemoteAddressStrategy(AclUtils.getAddreeStrArray(remoteAddr, last));
            } else {
                String[] strArray = StringUtils.split(remoteAddr, ".");
                String four = strArray[3];
                if (!four.startsWith("{")) {
                    throw new AclException(String.format("MultipleRemoteAddressStrategy netaddress examine scope Exception netaddress", remoteAddr));
                }
                return new MultipleRemoteAddressStrategy(AclUtils.getAddreeStrArray(remoteAddr, four));
            }
        } else if (AclUtils.isComma(remoteAddr)) {
            return new MultipleRemoteAddressStrategy(StringUtils.split(remoteAddr, ","));
        } else if (AclUtils.isAsterisk(remoteAddr) || AclUtils.isMinus(remoteAddr)) {
            return new RangeRemoteAddressStrategy(remoteAddr);
        }
        return new OneRemoteAddressStrategy(remoteAddr);

    }

    public static class NullRemoteAddressStrategy implements RemoteAddressStrategy {
        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            return true;
        }

    }

    public static class BlankRemoteAddressStrategy implements RemoteAddressStrategy {
        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            return false;
        }

    }

    public static class MultipleRemoteAddressStrategy implements RemoteAddressStrategy {

        private final Set<String> multipleSet = new HashSet<>();

        public MultipleRemoteAddressStrategy(String[] strArray) {
            InetAddressValidator validator = InetAddressValidator.getInstance();
            for (String netaddress : strArray) {
                if (validator.isValidInet4Address(netaddress)) {
                    multipleSet.add(netaddress);
                } else if (validator.isValidInet6Address(netaddress)) {
                    multipleSet.add(AclUtils.expandIP(netaddress, 8));
                } else {
                    throw new AclException(String.format("Netaddress examine Exception netaddress is %s", netaddress));
                }
            }
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            InetAddressValidator validator = InetAddressValidator.getInstance();
            String whiteRemoteAddress = plainAccessResource.getWhiteRemoteAddress();
            if (validator.isValidInet6Address(whiteRemoteAddress)) {
                whiteRemoteAddress = AclUtils.expandIP(whiteRemoteAddress, 8);
            }
            return multipleSet.contains(whiteRemoteAddress);
        }

    }

    public static class OneRemoteAddressStrategy implements RemoteAddressStrategy {

        private String netaddress;

        public OneRemoteAddressStrategy(String netaddress) {
            this.netaddress = netaddress;
            InetAddressValidator validator = InetAddressValidator.getInstance();
            if (!(validator.isValidInet4Address(netaddress) || validator.isValidInet6Address(netaddress))) {
                throw new AclException(String.format("Netaddress examine Exception netaddress is %s", netaddress));
            }
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            String writeRemoteAddress = AclUtils.expandIP(plainAccessResource.getWhiteRemoteAddress(), 8).toUpperCase();
            return AclUtils.expandIP(netaddress, 8).toUpperCase().equals(writeRemoteAddress);
        }

    }

    public static class RangeRemoteAddressStrategy implements RemoteAddressStrategy {

        private String head;

        private int start;

        private int end;

        private int index;

        public RangeRemoteAddressStrategy(String remoteAddr) {
//            IPv6 Address
            if (AclUtils.isColon(remoteAddr)) {
                AclUtils.IPv6AddressCheck(remoteAddr);
                String[] strArray = StringUtils.split(remoteAddr, ":");
                for (int i = 1; i < strArray.length; i++) {
                    if (ipv6Analysis(strArray, i)) {
                        AclUtils.verify(remoteAddr, index - 1);
                        String preAddress = AclUtils.v6ipProcess(remoteAddr, strArray, index);
                        this.index = StringUtils.split(preAddress, ":").length;
                        this.head = preAddress;
                        break;
                    }
                }
            } else {
                String[] strArray = StringUtils.split(remoteAddr, ".");
                if (analysis(strArray, 1) || analysis(strArray, 2) || analysis(strArray, 3)) {
                    AclUtils.verify(remoteAddr, index - 1);
                    StringBuffer sb = new StringBuffer();
                    for (int j = 0; j < index; j++) {
                        sb.append(strArray[j].trim()).append(".");
                    }
                    this.head = sb.toString();
                }
            }
        }

        private boolean analysis(String[] strArray, int index) {
            String value = strArray[index].trim();
            this.index = index;
            if ("*".equals(value)) {
                setValue(0, 255);
            } else if (AclUtils.isMinus(value)) {
                if (value.indexOf("-") == 0) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netaddress examine scope Exception value %s ", value));

                }
                String[] valueArray = StringUtils.split(value, "-");
                this.start = Integer.valueOf(valueArray[0]);
                this.end = Integer.valueOf(valueArray[1]);
                if (!(AclUtils.isScope(end) && AclUtils.isScope(start) && start <= end)) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netaddress examine scope Exception start is %s , end is %s", start, end));
                }
            }
            return this.end > 0 ? true : false;
        }

        private boolean ipv6Analysis(String[] strArray, int index) {
            String value = strArray[index].trim();
            this.index = index;
            if ("*".equals(value)) {
                int min = Integer.parseInt("0", 16);
                int max = Integer.parseInt("ffff", 16);
                setValue(min, max);
            } else if (AclUtils.isMinus(value)) {
                if (value.indexOf("-") == 0) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netaddress examine scope Exception value %s ", value));
                }
                String[] valueArray = StringUtils.split(value, "-");
                this.start = Integer.parseInt(valueArray[0], 16);
                this.end = Integer.parseInt(valueArray[1], 16);
                if (!(AclUtils.isIPv6Scope(end) && AclUtils.isIPv6Scope(start) && start <= end)) {
                    throw new AclException(String.format("RangeRemoteAddressStrategy netaddress examine scope Exception start is %s , end is %s", start, end));
                }
            }
            return this.end > 0 ? true : false;
        }

        private void setValue(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean match(PlainAccessResource plainAccessResource) {
            String netAddress = plainAccessResource.getWhiteRemoteAddress();
            InetAddressValidator validator = InetAddressValidator.getInstance();
            if (validator.isValidInet4Address(netAddress)) {
                if (netAddress.startsWith(this.head)) {
                    String value;
                    if (index == 3) {
                        value = netAddress.substring(this.head.length());
                    } else if (index == 2) {
                        value = netAddress.substring(this.head.length(), netAddress.lastIndexOf('.'));
                    } else {
                        value = netAddress.substring(this.head.length(), netAddress.lastIndexOf('.', netAddress.lastIndexOf('.') - 1));
                    }
                    Integer address = Integer.valueOf(value);
                    if (address >= this.start && address <= this.end) {
                        return true;
                    }
                }
            } else if (validator.isValidInet6Address(netAddress)) {
                netAddress = AclUtils.expandIP(netAddress, 8).toUpperCase();
                if (netAddress.startsWith(this.head)) {
                    String value = netAddress.substring(5 * index, 5 * index + 4);
                    Integer address = Integer.parseInt(value, 16);
                    if (address >= this.start && address <= this.end) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
