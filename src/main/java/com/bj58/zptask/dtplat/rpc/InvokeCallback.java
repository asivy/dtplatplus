package com.bj58.zptask.dtplat.rpc;

import com.bj58.zptask.dtplat.rpc.netty.ResponseFuture;

/**
 * 异步调用应答回调接口
 */
public interface InvokeCallback {
    public void operationComplete(final ResponseFuture responseFuture);
}
