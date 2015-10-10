package com.bj58.zptask.dtplat.rpc.netty5;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.support.SystemClock;
import com.bj58.zptask.dtplat.exception.RemotingConnectException;
import com.bj58.zptask.dtplat.exception.RemotingSendRequestException;
import com.bj58.zptask.dtplat.exception.RemotingTimeoutException;
import com.bj58.zptask.dtplat.rpc.netty5.handler.ClientMessageHandler;
import com.bj58.zptask.dtplat.util.RemotingHelper;
import com.bj58.zptask.dtplat.util.SelectorUtil;
import com.google.inject.Singleton;

@Singleton
public class NettyClient {

    private static final Logger logger = Logger.getLogger(NettyClient.class);

    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup group;
    private ConcurrentHashMap<String, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();
    public static final ConcurrentHashMap<Long, ResponseFuture> responseTable = new ConcurrentHashMap<Long, ResponseFuture>(256);

    public NettyClient() {
        group = new NioEventLoopGroup();
    }

    public void start() {
        bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                //                ch.pipeline().addLast(new IdleStateHandler(1, 1, 5));
                ch.pipeline().addLast(MarshallingCodeCFactory.buildMarshallingDecoder());
                ch.pipeline().addLast(MarshallingCodeCFactory.buildMarshallingEncoder());
                ch.pipeline().addLast(new ClientMessageHandler());
            }
        });

        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                scanResponseTable(3000);
            }
        }, 5 * 1000, 5000, TimeUnit.MILLISECONDS);
    }

    private final Lock lockChannelTables = new ReentrantLock();

    /**
     * 新建通道
     * @param address
     * @return
     */
    private Channel createChannel(final String address) {
        ChannelWrapper cw = channelTables.get(address);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        try {
            if (this.lockChannelTables.tryLock(1000, TimeUnit.MILLISECONDS)) {

                boolean createNewConnection = false;
                cw = this.channelTables.get(address);
                if (cw != null) {
                    // channel正常
                    if (cw.isOK()) {
                        return cw.getChannel();
                    }
                    // 正在连接，退出锁等待
                    else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    }
                    // 说明连接不成功
                    else {
                        this.channelTables.remove(address);
                        createNewConnection = true;
                    }
                }
                // ChannelWrapper不存在
                else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(new InetSocketAddress(address.split(":")[0], Integer.parseInt(address.split(":")[1])));
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(address, cw);
                }
            }
        } catch (Exception e) {
        } finally {
            this.lockChannelTables.unlock();
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(10 * 1000l)) {
                if (cw.isOK()) {
                    return cw.getChannel();
                }
            }
        }

        return null;
    }

    /**
     * 发起同步调用
     * 其实后面也是异步的
     * 这个同步是指：可以携带结果返回
     * @param address
     * @param message
     * @param timeout
     * @return
     * @throws Exception
     */
    public NettyMessage invokeSync(String address, final NettyMessage message, int timeout) throws Exception, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        final Channel channel = this.createChannel(address);
        long start = System.currentTimeMillis();
        if (channel != null && channel.isActive()) {
            try {
                final ResponseFuture responseFuture = new ResponseFuture(message.getOpaque(), timeout);
                responseTable.put(message.getOpaque(), responseFuture);
                channel.writeAndFlush(message).addListener(new ChannelFutureListener() {
                    //什么时候会触发这一个接口呢
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        } else {
                            responseFuture.setSendRequestOK(false);
                        }
                        //无结果 返回原因
                        responseTable.remove(message.getOpaque());
                        responseFuture.setCause(f.cause());
                    }
                });

                NettyMessage result = responseFuture.waitResponse();
                if (null == result) {
                    if (responseFuture.isSendRequestOK()) {
                        throw new Exception("timeout ");
                    } else {
                        throw new Exception("请求失败");
                    }
                }
                System.out.println("use: " + (System.currentTimeMillis() - start));
                return result;
            } catch (Exception e) {
                throw e;
            }
        } else {
            closeChannel(channel, address);
            throw new RemotingConnectException(address);
        }
    }

    /**
     * 关闭资源
     * 
     */
    public void shutdown() {
        try {
            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(cw.getChannel(), null);
            }
            this.channelTables.clear();
            this.group.shutdownGracefully();
        } catch (Exception e) {
            logger.error("shutdown error", e);
        }
    }

    /**
     * 如果通道不可用  则应将其关闭 以免占用资源
     * @param channel
     * @param address
     */
    public void closeChannel(Channel channel, String address) {
        if (channel == null) {
            return;
        }
        logger.info("close channel " + channel.id());
        final String addrRemote = null == address ? RemotingHelper.parseChannelRemoteAddr(channel) : address;
        try {
            if (this.lockChannelTables.tryLock(2000, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);
                    if (null == prevCW) {
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                    }
                    SelectorUtil.closeChannel(channel);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }

    }

    /**
     * 描述所有的对外请求
     * 看是否有超时未处理情况
     * 此超时时间已经考虑了 任务的默认等待时间
     */
    public void scanResponseTable(long timeout) {
        Iterator<Entry<Long, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + timeout) <= SystemClock.now()) {
                logger.info(String.format("remove responsefuture %d", rep.getOpaque()));
                it.remove();
            }
        }
    }

}
