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

package org.apache.rocketmq.oms.api.impl.rocketmq;

import io.openmessaging.api.Credentials;
import io.openmessaging.api.LifeCycle;
import io.openmessaging.api.exception.OMSRuntimeException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.oms.api.PropertyKeyConst;
import org.apache.rocketmq.oms.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.oms.api.impl.util.NameAddrUtils;

import static org.apache.rocketmq.common.UtilAll.getPid;

public abstract class OMSClientAbstract implements LifeCycle, Credentials {

    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    protected final Properties properties;
    protected final SessionCredentials sessionCredentials = new SessionCredentials();
    protected String nameServerAddr = NameAddrUtils.getNameAdd();

    protected TraceDispatcher traceDispatcher = null;

    protected final AtomicBoolean started = new AtomicBoolean(false);

    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ONSClient-UpdateNameServerThread");
            }
        });

    public OMSClientAbstract(Properties properties) {
        this.properties = properties;
        this.sessionCredentials.updateContent(properties);

        this.nameServerAddr = getNameSrvAddrFromProperties();
        if (nameServerAddr != null) {
            return;
        }

        this.nameServerAddr = fetchNameServerAddr();
        if (null == nameServerAddr) {
            throw new OMSRuntimeException(FAQ.errorMessage("Can not find name server, May be your network problem.", FAQ.FIND_NS_FAILED));
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    String nsAddrs = fetchNameServerAddr();
                    if (nsAddrs != null && !OMSClientAbstract.this.nameServerAddr.equals(nsAddrs)) {
                        OMSClientAbstract.this.nameServerAddr = nsAddrs;
                        if (isStarted()) {
                            updateNameServerAddr(nsAddrs);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("update name server periodically failed.", e);
                }
            }
        }, 10 * 1000L, 90 * 1000L, TimeUnit.MILLISECONDS);

    }

    protected abstract void updateNameServerAddr(String newAddrs);

    private String getNameSrvAddrFromProperties() {
        String nameserverAddrs = this.properties.getProperty(PropertyKeyConst.NAMESRV_ADDR);
        if (StringUtils.isNotEmpty(nameserverAddrs) && NameAddrUtils.NAMESRV_ENDPOINT_PATTERN.matcher(nameserverAddrs.trim()).matches()) {
            return nameserverAddrs.substring(NameAddrUtils.ENDPOINT_PREFIX.length());
        }

        return nameserverAddrs;
    }

    private String fetchNameServerAddr() {
        String nsAddrs = null;

        {
            String property = this.properties.getProperty(PropertyKeyConst.ONS_ADDR);
            if (property != null) {
                nsAddrs = new TopAddressing(property).fetchNSAddr();
                if (nsAddrs != null) {
                    LOGGER.info("connected to user-defined ons addr server, {} success, {}", property, nsAddrs);
                    return nsAddrs;
                } else {
                    throw new OMSRuntimeException(FAQ.errorMessage("Can not find name server with onsAddr " + property, FAQ.FIND_NS_FAILED));
                }
            }
        }

        return nsAddrs;
    }

    public String getNameServerAddr() {
        return this.nameServerAddr;
    }

    protected String buildIntanceName() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(Integer.toString(UtilAll.getPid())).append("#");
        if (this.nameServerAddr != null) {
            stringBuilder.append(this.nameServerAddr.hashCode()).append("#");
        }
        if (this.sessionCredentials.getAccessKey() != null) {
            stringBuilder.append(this.sessionCredentials.getAccessKey().hashCode()).append("#");
        }
        stringBuilder.append(System.nanoTime());
        return stringBuilder.toString();
    }

    protected String getNamespace() {
        String namespace = null;

        {
            String nameserverAddr = this.properties.getProperty(PropertyKeyConst.NAMESRV_ADDR);
            if (StringUtils.isNotEmpty(nameserverAddr)) {
                if (NameAddrUtils.validateInstanceEndpoint(nameserverAddr)) {
                    namespace = NameAddrUtils.parseInstanceIdFromEndpoint(nameserverAddr);
                    LOGGER.info("User specify namespace by endpoint: {}.", namespace);
                }
            }
        }

        {
            String namespaceFromProperty = this.properties.getProperty(PropertyKeyConst.INSTANCE_ID, null);
            if (StringUtils.isNotEmpty(namespaceFromProperty)) {
                namespace = namespaceFromProperty;
                LOGGER.info("User specify namespace by property: {}.", namespace);
            }
        }

        return namespace;
    }

    protected void checkONSProducerServiceState(DefaultMQProducerImpl producer) {
        switch (producer.getServiceState()) {
            case CREATE_JUST:
                throw new OMSRuntimeException(
                    FAQ.errorMessage(String.format("You do not have start the producer[" + getPid() + "], %s", producer.getServiceState()),
                        FAQ.SERVICE_STATE_WRONG));
            case SHUTDOWN_ALREADY:
                throw new OMSRuntimeException(FAQ.errorMessage(String.format("Your producer has been shut down, %s", producer.getServiceState()),
                    FAQ.SERVICE_STATE_WRONG));
            case START_FAILED:
                throw new OMSRuntimeException(FAQ.errorMessage(
                    String.format("When you start your service throws an exception, %s", producer.getServiceState()), FAQ.SERVICE_STATE_WRONG));
            case RUNNING:
                break;
            default:
                break;
        }
    }

    @Override
    public void start() {
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start();
            } catch (MQClientException e) {
                LOGGER.warn("trace dispatcher start failed ", e);
            }
        }
    }

    @Override
    public void updateCredential(Properties credentialProperties) {
        this.sessionCredentials.updateContent(credentialProperties);
    }

    @Override
    public void shutdown() {
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
        scheduledExecutorService.shutdown();
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    @Override
    public boolean isClosed() {
        return !isStarted();
    }
}
