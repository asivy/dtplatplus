package com.bj58.zptask.dtplat.core.listener;

import java.util.List;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.EcTopic;

/**
 * 用来监听自己的节点信息变化
 *
 * 所有的监听  都分的好散
 * 
 * 
 */
public class SelfChangeListener implements NodeChangeListener {

    private Config config;

    public SelfChangeListener() {
        config = InjectorHolder.getInstance(Config.class);

    }

    private void change(Node node) {
        if (node.getIdentity().equals(config.getIdentity())) {
            // 是当前节点, 看看节点配置是否发生变化
            // 1. 看 threads 有没有改变 , 目前只有 TASK_TRACKER 对 threads起作用
            //            if (node.getNodeType().equals(NodeType.TASK_TRACKER) && (node.getThreads() != config.getWorkThreads())) {
            //                config.setWorkThreads(node.getThreads());
            //                application.getEventCenter().publishAsync(new EventInfo(EcTopic.WORK_THREAD_CHANGE));
            //            }
            //            
            // 2. 看 available 有没有改变
            if (node.isAvailable() != config.isAvailable()) {
                String topic = node.isAvailable() ? EcTopic.NODE_ENABLE : EcTopic.NODE_DISABLE;
                config.setAvailable(node.isAvailable());
                //                application.getEventCenter().publishAsync(new EventInfo(topic));
                //改成eventbus 的事件定阅
            }
        }
    }

    @Override
    public void addNodes(List<Node> nodes) {
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }
        for (Node node : nodes) {
            change(node);
        }
    }

    @Override
    public void removeNodes(List<Node> nodes) {

    }
}
