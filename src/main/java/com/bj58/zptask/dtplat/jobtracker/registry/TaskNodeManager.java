package com.bj58.zptask.dtplat.jobtracker.registry;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelManager;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelWrapper;
import com.bj58.zptask.dtplat.jobtracker.domain.TaskTrackerNode;
import com.bj58.zptask.dtplat.util.ConcurrentHashSet;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Singleton;

/**
 * 管理所有的TaskNode节点
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午7:47:15
 * @see 
 * @since
 */
@Singleton
public class TaskNodeManager {

    private static final Logger logger = Logger.getLogger(TaskNodeManager.class);

    private final ConcurrentHashMap<String/*nodeGroup*/, Set<TaskTrackerNode>> NODE_MAP = new ConcurrentHashMap<String, Set<TaskTrackerNode>>();

    public Set<String> getNodeGroups() {
        return NODE_MAP.keySet();
    }

    /**
     * 添加节点
     *
     * @param node
     */
    @Subscribe
    public void addNode(Node node) {
        //  channel 可能为 null
        ChannelManager channelManager = InjectorHolder.getInstance(ChannelManager.class);
        ChannelWrapper channel = channelManager.getChannel(node.getGroup(), node.getNodeType(), node.getIdentity());
        Set<TaskTrackerNode> taskTrackerNodes = NODE_MAP.get(node.getGroup());

        if (taskTrackerNodes == null) {
            taskTrackerNodes = new ConcurrentHashSet<TaskTrackerNode>();
            Set<TaskTrackerNode> oldSet = NODE_MAP.putIfAbsent(node.getGroup(), taskTrackerNodes);
            if (oldSet != null) {
                taskTrackerNodes = oldSet;
            }
        }

        TaskTrackerNode taskTrackerNode = new TaskTrackerNode(node.getGroup(), node.getThreads(), node.getIdentity(), channel);
        logger.info(String.format("Add TaskTracker node:%s", taskTrackerNode));
        taskTrackerNodes.add(taskTrackerNode);

        InjectorHolder.getInstance(DTaskProvider.class).addNodeGroup(NodeType.TASK_TRACKER.toString(), node.getGroup());
    }

    /**
     * 删除节点
     *
     * @param node
     */
    @Subscribe
    public void removeNode(Node node) {
        Set<TaskTrackerNode> taskTrackerNodes = NODE_MAP.get(node.getGroup());
        if (taskTrackerNodes != null && taskTrackerNodes.size() != 0) {
            TaskTrackerNode taskTrackerNode = new TaskTrackerNode(node.getIdentity());
            taskTrackerNode.setNodeGroup(node.getGroup());
            logger.info(String.format("Remove TaskTracker node:%s", taskTrackerNode));
            taskTrackerNodes.remove(taskTrackerNode);
        }
    }

    public TaskTrackerNode getTaskTrackerNode(String nodeGroup, String identity) {
        Set<TaskTrackerNode> taskTrackerNodes = NODE_MAP.get(nodeGroup);
        if (taskTrackerNodes == null || taskTrackerNodes.size() == 0) {
            return null;
        }

        ChannelManager channelManager = InjectorHolder.getInstance(ChannelManager.class);

        for (TaskTrackerNode taskTrackerNode : taskTrackerNodes) {
            if (taskTrackerNode.getIdentity().equals(identity)) {
                if (taskTrackerNode.getChannel() == null || taskTrackerNode.getChannel().isClosed()) {
                    // 如果 channel 已经关闭, 更新channel, 如果没有channel, 略过
                    ChannelWrapper channel = channelManager.getChannel(taskTrackerNode.getNodeGroup(), NodeType.TASK_TRACKER, taskTrackerNode.getIdentity());
                    if (channel != null) {
                        // 更新channel
                        taskTrackerNode.setChannel(channel);
                        logger.info(String.format("update node channel , taskTackerNode=%s", taskTrackerNode));
                        return taskTrackerNode;
                    }
                } else {
                    // 只有当channel正常的时候才返回
                    return taskTrackerNode;
                }
            }
        }
        return null;
    }

}
