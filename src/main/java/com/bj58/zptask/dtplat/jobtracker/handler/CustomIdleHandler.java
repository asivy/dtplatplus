package com.bj58.zptask.dtplat.jobtracker.handler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;

/**
 * 检测空闲连接
 * 
 * 问题是 这个与心跳 是否有冲突呢
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月14日 下午8:02:40
 * @see 
 * @since
 */
public class CustomIdleHandler extends ChannelHandlerAdapter {

    private static final Logger logger = Logger.getLogger(CustomIdleHandler.class);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                logger.info(String.format("Channed Read Idle ,id=%s", ctx.channel().id()));
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE) {
                NettyMessage msg = new NettyMessage();
                msg.setTimestamp(System.currentTimeMillis());
                msg.setType(MessageType.HEARTBEAT_RES.value());
                msg.setJsonobj(String.format("Channed Write Idle ,id=%s", ctx.channel().id()));
                logger.info(String.format("Channed Write Idle ,id=%s", ctx.channel().id()));
                ctx.writeAndFlush(msg);
            }
        }
    }

}
