/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mycat.netty;

import org.mycat.netty.util.SysProperties;

import org.mycat.netty.util.ExtendableThreadPoolExecutor;
import org.mycat.netty.util.ExtendableThreadPoolExecutor.TaskQueue;
import org.mycat.netty.util.Threads;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class NettyServer {

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /**
     * The default port to use for the server.
     */
    public static final int DEFAULT_LISTEN_PORT = 6100;

    private ServerArgs args;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor userExecutor;
    private ChannelFuture f;

    public NettyServer(ServerArgs args) {
        this.args = args;
    }

    /**
     * Listen for incoming connections.
     */
    public void init() {

        logger.debug("nettyServer init");
        try {
//            if (!StringUtils.isNullOrEmpty(args.configFile)) {
//                System.setProperty("ddal.engineConfigLocation", args.configFile);
//            }
            logger.info("{} server init ddal-engine from {}", getServerName(), SysProperties.ENGINE_CONFIG_LOCATION);
            Properties prop = new Properties();
            logger.info("{} server ddal-engine inited.", getServerName());
        } catch (Exception e) {
            logger.error("Exception happen when init ddal-engine ", e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Listen for incoming connections.
     */
    public void listen() {
        logger.info("{} server is starting", getServerName());
        args.validate();
        ServerBootstrap b = configServer();
        try {
            // start server
            f = b.bind(args.port).sync();
            logger.info("{} server started and listening on {}", getServerName(), args.port);
            // register shutown hook
            Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        } catch (Exception e) {
            logger.error("Exception happen when start " + getServerName(), e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * blocking to wait for close.
     */
    public void waitForClose() throws InterruptedException {
        f.channel().closeFuture().sync();
    }

    public void stop() {
        logger.info("{} server is stopping", getServerName());
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        int timeoutMills = args.shutdownTimeoutMills;
        Threads.shutdownGracefully(userExecutor, timeoutMills, timeoutMills, TimeUnit.SECONDS);
        logger.info("{} server stoped", getServerName());
    }

    private ServerBootstrap configServer() {
        bossGroup = new NioEventLoopGroup(args.bossThreads, new DefaultThreadFactory("NettyBossGroup", true));
        workerGroup = new NioEventLoopGroup(args.workerThreads, new DefaultThreadFactory("NettyWorkerGroup", true));
        userExecutor = createUserThreadExecutor();
        
        final ProtocolHandler handshakeHandler = newHandshakeHandler(userExecutor);
        final ProtocolHandler protocolHandler = newProtocolHandler(userExecutor);

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_REUSEADDR, true).childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true);

        if (args.socketTimeoutMills > 0) {
            b.childOption(ChannelOption.SO_TIMEOUT, args.socketTimeoutMills);
        }

        if (args.recvBuff > 0) {
            b.childOption(ChannelOption.SO_RCVBUF, args.recvBuff);
        }

        if (args.sendBuff > 0) {
            b.childOption(ChannelOption.SO_SNDBUF, args.sendBuff);
        }

        b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(createProtocolDecoder(),
                        /* createProtocolEncoder(), */ handshakeHandler, protocolHandler);
            }
        });

        return b;
    }


    public ThreadPoolExecutor createUserThreadExecutor() {
        TaskQueue queue = new TaskQueue(SysProperties.THREAD_QUEUE_SIZE);
        int poolCoreSize = SysProperties.THREAD_POOL_SIZE_CORE;
        int poolMaxSize = SysProperties.THREAD_POOL_SIZE_MAX;
        poolMaxSize = poolMaxSize > poolCoreSize ? poolMaxSize : poolCoreSize;
        ExtendableThreadPoolExecutor userExecutor = new ExtendableThreadPoolExecutor(poolCoreSize, poolMaxSize, 5L,
                TimeUnit.MINUTES, queue, Threads.newThreadFactory("request-processor"));
        return userExecutor;
    }
    
    private ProtocolHandler newHandshakeHandler(ThreadPoolExecutor userExecutor) {
        ProtocolHandler handshakeHandler = createHandshakeHandler();
        handshakeHandler.setUserExecutor(userExecutor);
        return handshakeHandler;
    }
    
    private ProtocolHandler newProtocolHandler(ThreadPoolExecutor userExecutor) {
        ProtocolHandler protocolHandler = createProtocolHandler();
        protocolHandler.setUserExecutor(userExecutor);
        return protocolHandler;
    }


    protected abstract String getServerName();

    protected abstract ChannelHandler createProtocolDecoder();

    protected abstract ProtocolHandler createHandshakeHandler();
    
    protected abstract ProtocolHandler createProtocolHandler();

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            NettyServer.this.stop();
        }
    }
}
