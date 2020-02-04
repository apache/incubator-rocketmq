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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock lockNamesrvChannel = new ReentrantLock();

    private final ExecutorService publicExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    private final ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt() % 999) % 999;
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyClientConfig.getClientWorkerThreads(),
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });

        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            log.info("Prepend SSL handler");
                        } else {
                            log.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    pipeline.addLast(
                        defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel)
            return;

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    if (null == prevCW) {
                        log.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        log.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel)
            return;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);
            }
        }
    }

    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    private Channel getAndCreateChannel(final String addr) throws RemotingConnectException, InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }

    private Channel getAndCreateNameserverChannel() throws RemotingConnectException, InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    throw new RemotingConnectException(addrList.toString());
                }
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private Channel createChannel(final String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection;
                cw = this.channelTables.get(addr);
                if (cw != null) {

                    if (cw.isOK()) {
                        return cw.getChannel();
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString(), channelFuture.cause());
                }
            } else {
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                    channelFuture.toString());
            }
        }

        return null;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTooMuchRequestException("invokeAsync call timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
