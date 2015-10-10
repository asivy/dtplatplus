package com.bj58.zptask.dtplat.rpc.netty5;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class ChannelWrapper {

    private final ChannelFuture channelFuture;

    public ChannelWrapper(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    public boolean isOK() {
        return (this.channelFuture.channel() != null && this.channelFuture.channel().isActive());
    }

    public Channel getChannel() {
        return this.channelFuture.channel();
    }

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }
}
