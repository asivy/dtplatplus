package com.bj58.zptask.dtplat.rpc.netty5;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步请求应答封装
 */
public class ResponseFuture {
    private final long opaque;
    private final long timeoutMillis;
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    // 保证信号量至多至少只被释放一次
    // 保证回调的callback方法至多至少只被执行一次
    //    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile boolean sendRequestOK = true;
    private volatile NettyMessage message;

    private volatile Throwable cause;

    public ResponseFuture(long opaque, long timeoutMillis) {
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public NettyMessage waitResponse() throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.message;
    }

    public void putResponse(final NettyMessage message) {
        this.message = message;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public long getOpaque() {
        return opaque;
    }

}
