package com.bj58.zptask.dtplat.jobtracker.handler;

import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.util.SerializeUtil;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

/**
 * 再这么写下去  handler会爆炸的  
 * 基本上有一个接口就有一个HANDLER了
 * 
 * 所以不能这样做
 * scf走的是接口-实现 互调的方式
 * lts走的是   procesor的方式   虽不通用  但快
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月8日 上午11:33:26
 * @see 
 * @since
 */
public class TashExecutingHandler extends ChannelHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        NettyMessage msg = (NettyMessage) obj;

        if (msg.getType() == MessageType.TASK_PULL.value()) {
            msg.setTimestamp(System.currentTimeMillis());
            String taskGroup = msg.getNodeGroup();
            String taskIdentity = msg.getIdentity();
            TaskExecutableBean task = InjectorHolder.getInstance(DTaskProvider.class).takeExecutaleTask(taskGroup, taskIdentity);
            if (task != null) {
                msg.setJsonobj(SerializeUtil.jsonSerialize(task));
            }
            ctx.writeAndFlush(msg);
        } else {
            ctx.fireChannelRead(obj);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
