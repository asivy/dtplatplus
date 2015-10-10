package com.bj58.zptask.dtplat.jobtracker.support.listener;

import java.util.List;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.core.listener.NodeChangeListener;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.util.CollectionUtils;

/**
 * 节点变化监听器
 */
public class JobNodeChangeListener implements NodeChangeListener {

    private static final Logger logger = Logger.getLogger(JobNodeChangeListener.class);

    private JobTrackerApplication application;

    public JobNodeChangeListener(JobTrackerApplication application) {
        this.application = application;
    }
    
    @Override
    public void addNodes(List<Node> nodes) {
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }
        for (Node node : nodes) {
            logger.info(String.format("%s add node %s", node.getGroup(), node.getIdentity()));
            if (node.getNodeType().equals(NodeType.TASK_TRACKER)) {
                application.getTaskTrackerManager().addNode(node);
            } else if (node.getNodeType().equals(NodeType.JOB_CLIENT)) {
                application.getJobClientManager().addNode(node);
            }
        }
    }

    @Override
    public void removeNodes(List<Node> nodes) {
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }
        for (Node node : nodes) {
            logger.info(String.format("%s delete node %s", node.getGroup(), node.getIdentity()));
            if (node.getNodeType().equals(NodeType.TASK_TRACKER)) {
                application.getTaskTrackerManager().removeNode(node);
                application.getExecutingDeadJobChecker().fixedDeadNodeJob(node);
            } else if (node.getNodeType().equals(NodeType.JOB_CLIENT)) {
                application.getJobClientManager().removeNode(node);
            }
        }
    }
}
