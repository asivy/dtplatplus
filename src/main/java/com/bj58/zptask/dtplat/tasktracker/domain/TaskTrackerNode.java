package com.bj58.zptask.dtplat.tasktracker.domain;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;

/**
 * @author Robert HG (254963746@qq.com) on 8/14/14.
 * TaskTracker 节点
 */
public class TaskTrackerNode extends Node {

    public TaskTrackerNode() {
        this.setNodeType(NodeType.TASK_TRACKER);
        // 关注 JobTracker
        this.addListenNodeType(NodeType.JOB_TRACKER.toString());
        this.addListenNodeType(NodeType.TASK_TRACKER.toString());
    }

}
