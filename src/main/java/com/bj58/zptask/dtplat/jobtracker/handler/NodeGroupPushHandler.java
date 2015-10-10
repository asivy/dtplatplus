package com.bj58.zptask.dtplat.jobtracker.handler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import com.bj58.zhaopin.feature.entity.NodeGroupBean;
import com.bj58.zhaopin.feature.util.StringUtil;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.util.SerializeUtil;

public class NodeGroupPushHandler extends ChannelHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        NettyMessage msg = (NettyMessage) obj;
        System.out.println("receive nodegroup push request");
        if (msg.getType() == MessageType.NodeGroupPush.value()) {
            msg.setTimestamp(System.currentTimeMillis());
            String nodetype = msg.getNodeType();
            String nodegroup = msg.getNodeGroup();
            if (!StringUtil.isNullOrEmpty(nodetype) && !StringUtil.isNullOrEmpty(nodegroup)) {
                InjectorHolder.getInstance(DTaskProvider.class).addNodeGroup(nodetype, nodegroup);
            }
            NodeGroupBean node = InjectorHolder.getInstance(DTaskProvider.class).getINodeGroupService().loadByTypeName(nodetype, nodegroup);
            if (node != null) {
                msg.setJsonobj(SerializeUtil.jsonSerialize(node));
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
