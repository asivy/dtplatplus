package com.bj58.zptask.dtplat.core.listener;

import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 *  用来监听 自己类型 节点的变化,用来选举master
 *  监听的是自已所在的组
 *  以便对MASTER迅速做出响应
 */
public class MasterElectionListener implements NodeChangeListener {

    private Application application;

    public MasterElectionListener(Application application) {
        this.application = application;
    }
    
    public void removeNodes(List<Node> nodes) {
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }
        // 只需要和当前节点相同的节点类型和组
        List<Node> groupNodes = new ArrayList<Node>();
        for (Node node : nodes) {
            if (isSameGroup(node)) {
                groupNodes.add(node);
            }
        }
        if (groupNodes.size() > 0) {
            application.getMasterElector().removeNode(groupNodes);
        }
    }

    public void addNodes(List<Node> nodes) {
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }
        // 只需要和当前节点相同的节点类型和组
        List<Node> groupNodes = new ArrayList<Node>();
        for (Node node : nodes) {
            if (isSameGroup(node)) {
                groupNodes.add(node);
            }
        }
        if (groupNodes.size() > 0) {
            application.getMasterElector().addNodes(groupNodes);
        }
    }

    /**
     * 是否和当前节点是相同的GROUP
     *
     * @param node
     * @return
     */
    private boolean isSameGroup(Node node) {
        return node.getNodeType().equals(application.getConfig().getNodeType()) && node.getGroup().equals(application.getConfig().getNodeGroup());
    }

}
