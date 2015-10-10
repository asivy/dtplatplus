package com.bj58.zptask.dtplat.rpc.netty;

import com.bj58.zptask.dtplat.exception.RemotingCommandException;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

/**
 * 接收请求处理器，服务器与客户端通用
 * 
 * 
 * 有handler了还这样 没必要的了吧
 * 
 * 
 */
public interface NettyRequestProcessor {
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException;
}
