package com.bj58.zptask.dtplat.rpc;

import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.exception.RemotingSendException;
import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;

import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

/**
 * 对remotingserver的包装
 * 
 * 感觉架构好乱
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 上午10:30:02
 * @see 
 * @since
 */
public class RemotingServerDelegate {

    private RemotingServer remotingServer;
    private Application application;

    public RemotingServerDelegate(RemotingServer remotingServer, Application application) {
        this.remotingServer = remotingServer;
        this.application = application;
    }
    
    public void start() {
        try {
            remotingServer.start();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        remotingServer.registerProcessor(requestCode, processor, executor);
    }

    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        remotingServer.registerDefaultProcessor(processor, executor);
    }

    public RemotingCommand invokeSync(Channel channel, RemotingCommand request) throws RemotingSendException {
        try {

            return remotingServer.invokeSync(channel, request, application.getConfig().getInvokeTimeoutMillis());
        } catch (Throwable t) {
            throw new RemotingSendException(t);
        }
    }

    public void invokeAsync(Channel channel, RemotingCommand request, InvokeCallback invokeCallback) throws RemotingSendException {
        try {

            remotingServer.invokeAsync(channel, request, application.getConfig().getInvokeTimeoutMillis(), invokeCallback);
        } catch (Throwable t) {
            throw new RemotingSendException(t);
        }
    }

    public void invokeOneway(Channel channel, RemotingCommand request) throws RemotingSendException {
        try {

            remotingServer.invokeOneway(channel, request, application.getConfig().getInvokeTimeoutMillis());
        } catch (Throwable t) {
            throw new RemotingSendException(t);
        }
    }

    public void shutdown() {
        remotingServer.shutdown();
    }
}
