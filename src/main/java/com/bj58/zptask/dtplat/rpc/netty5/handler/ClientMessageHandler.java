package com.bj58.zptask.dtplat.rpc.netty5.handler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyClient;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.rpc.netty5.ResponseFuture;

public class ClientMessageHandler extends ChannelHandlerAdapter {

    private static final Logger logger = Logger.getLogger(ClientMessageHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        NettyMessage message = (NettyMessage) msg;
        if (message.getType() == MessageType.HEARTBEAT_RES.value()) {
            logger.info(message.getJsonobj());
        } else {
            final ResponseFuture responseFuture = NettyClient.responseTable.get(message.getOpaque());
            if (responseFuture != null) {
                responseFuture.putResponse(message);
            }
            NettyClient.responseTable.remove(message.getOpaque());
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
