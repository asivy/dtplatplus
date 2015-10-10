package com.bj58.zptask.dtplat.jobtracker.registry;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.registry.event.CustomEventBus;
import com.bj58.zptask.dtplat.registry.event.NodeAddEvent;
import com.bj58.zptask.dtplat.registry.event.NodeRemoveEvent;
import com.bj58.zptask.dtplat.zookeeper.NodePathHelper;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Singleton;

/**
 * 监听Task节点的变化
 * 所有的节点存在TaskNodeManager中
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午7:36:52
 * @see 
 * @since
 */
@Singleton
public class TaskNodeChangeBus implements CustomEventBus {

    private static final Logger logger = Logger.getLogger(TaskNodeChangeBus.class);

    @Subscribe
    public void addNode(NodeAddEvent event) {
        Preconditions.checkNotNull(event);
        Node node = NodePathHelper.parse(event.getPath());
        if (node.getNodeType().equals(NodeType.TASK_TRACKER)) {
            logger.info(String.format("%s add node %s", node.getGroup(), node.getIdentity()));
            TaskNodeManager taskManager = InjectorHolder.getInstance(TaskNodeManager.class);
            taskManager.addNode(node);
        }
    }

    @Subscribe
    public void removeNode(NodeRemoveEvent event) {
        Preconditions.checkNotNull(event);
        Node node = NodePathHelper.parse(event.getPath());
        if (node.getNodeType().equals(NodeType.TASK_TRACKER)) {
            logger.info(String.format("%s delete node %s", node.getGroup(), node.getIdentity()));
            TaskNodeManager taskManager = InjectorHolder.getInstance(TaskNodeManager.class);
            taskManager.removeNode(node);
            InjectorHolder.getInstance(ExecutingTaskCheck.class).fixDeadTaskByNode(node);
        }
    }

}
