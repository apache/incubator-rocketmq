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

package org.apache.rocketmq.remoting.netty;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class NettyClientConfigTest {

  @Test
  public void testChangeConfigBySystemProperty() throws NoSuchFieldException, IllegalAccessException {
    

    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE, "1");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT, "2000");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS, "60");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "16383");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "16384");
    System.setProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT, "false");


    NettySystemConfig.socketSndbufSize =
        Integer.parseInt(System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "65535"));
    NettySystemConfig.socketRcvbufSize =
        Integer.parseInt(System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "65535"));
    NettySystemConfig.clientWorkerSize =
        Integer.parseInt(System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE, "4"));
    NettySystemConfig.connectTimeoutMillis =
        Integer.parseInt(System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT, "3000"));
    NettySystemConfig.clientChannelMaxIdleTimeSeconds =
        Integer.parseInt(System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CHANNEL_MAX_IDLE_SECONDS, "120"));
    NettySystemConfig.clientCloseSocketIfTimeout =
        Boolean.parseBoolean(System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_CLIENT_CLOSE_SOCKET_IF_TIMEOUT, "true"));

    NettyClientConfig changedConfig = new NettyClientConfig();
    assertThat(changedConfig.getClientWorkerThreads()).isEqualTo(1);
    assertThat(changedConfig.getClientOnewaySemaphoreValue()).isEqualTo(65535);
    assertThat(changedConfig.getClientAsyncSemaphoreValue()).isEqualTo(65535);
    assertThat(changedConfig.getConnectTimeoutMillis()).isEqualTo(2000);
    assertThat(changedConfig.getClientChannelMaxIdleTimeSeconds()).isEqualTo(60);
    assertThat(changedConfig.getClientSocketSndBufSize()).isEqualTo(16383);
    assertThat(changedConfig.getClientSocketRcvBufSize()).isEqualTo(16384);
    assertThat(changedConfig.isClientCloseSocketIfTimeout()).isEqualTo(false);
  }
}
