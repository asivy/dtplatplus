package com.bj58.zptask.dtplat.test.netty;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.util.StringUtil;
import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyClient;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.util.SerializeUtil;

public class ClientTest {

    public static void main(String[] args) {
        try {
            NettyClient client = new NettyClient();
            client.start();

            for (int i = 0; i < 20000; i++) {
                NettyMessage message = new NettyMessage();
                message.setType(MessageType.TASK_PULL.value());
                message.setNodeGroup("track_group");
                message.setIdentity("test-task");
                try {
                    NettyMessage msg = client.invokeSync("127.0.0.1:6061", message, 10000);
                    if (!StringUtil.isNullOrEmpty(msg.getJsonobj())) {
                        TaskExecutableBean task = SerializeUtil.jsonDeserialize(msg.getJsonobj());
                        if (task != null) {
                            System.out.println(JSON.toJSONString(task));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void nodegroup() {
        try {
            NettyClient client = new NettyClient();
            NettyMessage message = new NettyMessage();
            message.setType(MessageType.NodeGroupPush.value());
            message.setNodeType("TASK_TRACKER");
            message.setNodeGroup("daitoubootstrap");
            for (int i = 0; i < 1; i++) {
                Thread.sleep(1000);
                NettyMessage msg = client.invokeSync("127.0.0.1:6061", message, 10000);
                System.out.println(msg.getTimestamp());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
