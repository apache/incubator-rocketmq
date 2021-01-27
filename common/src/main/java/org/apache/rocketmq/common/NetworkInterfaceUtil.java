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
package org.apache.rocketmq.common;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

public class NetworkInterfaceUtil {

    private static final List<NetworkInterface> INTERFACES = new ArrayList<>();
    private static final Throwable THROWABLE;

    static {
        Throwable exception = null;
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            for (; networkInterfaces.hasMoreElements(); ) {
                INTERFACES.add(networkInterfaces.nextElement());
            }
        } catch (Throwable e) {
            exception = e;
        }
        THROWABLE = exception;
    }

    public static Enumeration<NetworkInterface> getNetworkInterfaces() throws SocketException {
        if (THROWABLE != null) {
            SocketException socketException = new SocketException("Get network interfaces failed");
            socketException.initCause(THROWABLE);
            throw socketException;
        } else {
            Iterator<NetworkInterface> iterator = INTERFACES.iterator();
            return new Enumeration<NetworkInterface>() {
                @Override
                public boolean hasMoreElements() {
                    return iterator.hasNext();
                }

                @Override
                public NetworkInterface nextElement() {
                    return iterator.next();
                }
            };
        }
    }

}
