package com.bj58.zptask.dtplat.jobtracker.domain;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;

/**
 * @author Robert HG (254963746@qq.com) on 7/23/14.
 * Job Tracker 节点
 */
public class JobTrackerNode extends Node {

    public JobTrackerNode() {
        this.setNodeType(NodeType.JOB_TRACKER);
        this.addListenNodeType(NodeType.TASK_TRACKER.toString());
        //        this.addListenNodeType(NodeType.JOB_TRACKER.toString());
    }
}
