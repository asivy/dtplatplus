package com.bj58.zptask.dtplat.jobtracker.handler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import com.bj58.zhaopin.feature.entity.TaskLogBean;
import com.bj58.zhaopin.feature.util.StringUtil;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.util.SerializeUtil;

/**
 * 记录操作行为日志
 * 
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月15日 下午7:30:06
 * @see 
 * @since
 */
public class TaskLogHandler extends ChannelHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        NettyMessage msg = (NettyMessage) obj;
        if (msg.getType() == MessageType.TASK_LOG.value()) {
            ctx.writeAndFlush(processMsg(msg));
        } else {
            ctx.fireChannelRead(obj);
        }
    }

    /**
     * 存储任务日志
     * @param msg
     * @return
     */
    private NettyMessage processMsg(NettyMessage msg) {
        msg.setTimestamp(System.currentTimeMillis());
        String json = msg.getJsonobj();
        if (StringUtil.isNullOrEmpty(json)) {
        } else {
            TaskLogBean taskLog = SerializeUtil.jsonDeserialize(json);
            InjectorHolder.getInstance(DTaskProvider.class).addTaskLog(taskLog);
        }
        msg.setType(MessageType.TASK_LOG_RES.value());
        msg.setJsonobj("");
        return msg;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
