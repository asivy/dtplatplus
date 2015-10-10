package com.bj58.zptask.dtplat.rpc.netty5;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.support.SystemClock;
import com.bj58.zptask.dtplat.jobtracker.handler.CustomIdleHandler;
import com.bj58.zptask.dtplat.jobtracker.handler.NodeGroupPushHandler;
import com.bj58.zptask.dtplat.jobtracker.handler.TaskPullHandler;
import com.bj58.zptask.dtplat.util.GenericsUtils;
import com.google.inject.Singleton;

@Singleton
public class NettyServer {

    private static final Logger logger = Logger.getLogger(NettyServer.class);

    private final ServerBootstrap bootstrap = new ServerBootstrap();
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ScheduledExecutorService respScheduler;
    public static final ConcurrentHashMap<Integer, ResponseFuture> responseTable = GenericsUtils.newConcurrentHashMap();

    public NettyServer() {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        respScheduler = new ScheduledThreadPoolExecutor(1);

    }

    public void bind(int port) throws Exception {
        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);
        bootstrap.option(ChannelOption.SO_BACKLOG, 65536);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.localAddress(port);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws IOException {
                ch.pipeline().addLast("idleStateHandler", new IdleStateHandler(10, 5, 100));
                ch.pipeline().addLast("heartbeat", new CustomIdleHandler());
                ch.pipeline().addLast(MarshallingCodeCFactory.buildMarshallingDecoder());
                ch.pipeline().addLast(MarshallingCodeCFactory.buildMarshallingEncoder());
                ch.pipeline().addLast("taskpull", new TaskPullHandler());
                ch.pipeline().addLast("nodegroup", new NodeGroupPushHandler());
            }
        });
        
        this.bootstrap.bind().sync();
        respScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanResponseTable(5000);
            }
        }, 5 * 1000, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 描述所有的对外请求
     * 看是否有超时未处理情况
     * 此超时时间已经考虑了 任务的默认等待时间
     */
    public void scanResponseTable(long timeout) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + timeout) <= SystemClock.now()) {
                logger.info(String.format("remove responsefuture ", rep.getOpaque()));
                it.remove();
            }
        }
    }

    /**
     * 监听到虚拟机停止时调用
     * 当系统启动失败时也调用
     */
    public void shutdown() {
        try {
            logger.info("receive shutdown listener ");
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
                logger.info("shotdown bossGroup");
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
                logger.info("shotdown workerGroup");
            }
            if (respScheduler != null) {
                respScheduler.shutdown();
                logger.info("shotdown respScheduler");
            }
        } catch (Exception e) {
            logger.error("shutsown error", e);
        }

    }
}
