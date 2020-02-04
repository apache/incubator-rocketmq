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
package org.apache.rocketmq.common.message;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;

public class MessageClientIDSetter {
    private static final String TOPIC_KEY_SPLITTER = "#";
    private static final int LEN;
    private static final String FIX_STRING;
    private static final AtomicInteger COUNTER;
    private static long startTime;
    private static long nextStartTime;

    static {
        byte[] ip;
        try {
            ip = UtilAll.getIP();
        } catch (Exception e) {
            ip = createFakeIP();
        }
        LEN = ip.length + 2 + 4 + 4 + 2;
        ByteBuffer tempBuffer = ByteBuffer.allocate(ip.length + 2 + 4);
        tempBuffer.position(0);
        tempBuffer.put(ip);
        tempBuffer.position(ip.length);
        tempBuffer.putInt(UtilAll.getPid());
        tempBuffer.position(ip.length + 2);
        tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
        FIX_STRING = UtilAll.bytes2string(tempBuffer.array());
        setStartTime(System.currentTimeMillis());
        COUNTER = new AtomicInteger(0);
    }

    private synchronized static void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }

    public static Date getNearlyTimeFromID(String msgID) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        byte[] bytes = UtilAll.string2bytes(msgID);
        int ipLength = bytes.length == 28 ? 16 : 4;
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put(bytes, ipLength + 2 + 4, 4);
        buf.position(0);
        long spanMS = buf.getLong();
        Calendar cal = Calendar.getInstance();
        long now = cal.getTimeInMillis();
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long monStartTime = cal.getTimeInMillis();
        if (monStartTime + spanMS >= now) {
            cal.add(Calendar.MONTH, -1);
            monStartTime = cal.getTimeInMillis();
        }
        cal.setTimeInMillis(monStartTime + spanMS);
        return cal.getTime();
    }

    public static String getIPStrFromID(String msgID) {
        byte[] ipBytes = getIPFromID(msgID);
        if (ipBytes.length == 16) {
            return UtilAll.ipToIPv6Str(ipBytes);
        } else {
            return UtilAll.ipToIPv4Str(ipBytes);
        }
    }

    public static byte[] getIPFromID(String msgID) {
        byte[] bytes = UtilAll.string2bytes(msgID);
        int ipLength = bytes.length == 28 ? 16 : 4;
        byte[] result = new byte[ipLength];
        System.arraycopy(bytes, 0, result, 0, ipLength);
        return result;
    }

    public static String createUniqID() {
        StringBuilder sb = new StringBuilder(LEN * 2);
        sb.append(FIX_STRING);
        sb.append(UtilAll.bytes2string(createUniqIDBuffer()));
        return sb.toString();
    }

    private static byte[] createUniqIDBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 2);
        long current = System.currentTimeMillis();
        if (current >= nextStartTime) {
            setStartTime(current);
        }
        buffer.position(0);
        buffer.putInt((int) (System.currentTimeMillis() - startTime));
        buffer.putShort((short) COUNTER.getAndIncrement());
        return buffer.array();
    }

    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
        }
    }

    public static String getUniqID(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }

    public static byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];
        bb.get(fakeIP);
        return fakeIP;
    }
}
    
