package com.bj58.zptask.dtplat.rpc;

import com.bj58.zptask.dtplat.exception.RemotingConnectException;
import com.bj58.zptask.dtplat.exception.RemotingSendRequestException;
import com.bj58.zptask.dtplat.exception.RemotingTimeoutException;
import com.bj58.zptask.dtplat.exception.RemotingTooMuchRequestException;
import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;

import java.util.concurrent.ExecutorService;

/**
 * 远程通信，Client接口
 */
public interface RemotingClient {

    public void start();

    /**
     * 同步调用
     */
    public RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步调用
     */
    public void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向调用
     */
    public void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 注册处理器
     */
    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 注册默认处理器
     */
    public void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    public void shutdown();
}
