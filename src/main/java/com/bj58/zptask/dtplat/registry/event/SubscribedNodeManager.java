package com.bj58.zptask.dtplat.registry.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.ListUtils;

/**
 *  节点管理 (主要用于管理自己关注的节点)
 *  仅是本地的管理  并不涉及ZK
 *  
 *  没搞懂这个是类是做什么的
 */
public class SubscribedNodeManager implements CustomEventBus {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubscribedNodeManager.class);
    private final ConcurrentHashMap<NodeType, List<Node>> NODES = new ConcurrentHashMap<NodeType, List<Node>>();

    /**
     * 添加监听的节点
     * 
     * @param node
     */
    private void addNode(Node node) {
        final Config config = InjectorHolder.getInstance(Config.class);
        if ((NodeType.JOB_TRACKER.equals(node.getNodeType()))) {
            // 如果增加的JobTracker节点，那么直接添加，因为三种节点都需要监听
            _addNode(node);
        } else if (NodeType.JOB_TRACKER.equals(config.getNodeType())) {
            // 如果当天节点是JobTracker节点，那么直接添加，因为JobTracker节点要监听三种节点
            _addNode(node);
        } else if (config.getNodeType().equals(node.getNodeType()) && config.getNodeGroup().equals(node.getGroup())) {
            // 剩下这种情况是JobClient和TaskTracker都只监听和自己同一个group的节点
            _addNode(node);
        }
    }

    private void _addNode(Node node) {
        List<Node> nodeList = NODES.get(node.getNodeType());
        if (CollectionUtils.isEmpty(nodeList)) {
            nodeList = new CopyOnWriteArrayList<Node>();
            List<Node> oldNodeList = NODES.putIfAbsent(node.getNodeType(), nodeList);
            if (oldNodeList != null) {
                nodeList = oldNodeList;
            }
        }
        nodeList.add(node);
    }

    public List<Node> getNodeList(final NodeType nodeType, final String nodeGroup) {

        List<Node> nodes = NODES.get(nodeType);

        return ListUtils.filter(nodes, new ListUtils.Filter<Node>() {
            @Override
            public boolean filter(Node node) {
                return node.getGroup().equals(nodeGroup);
            }
        });
    }

    public List<Node> getNodeList(NodeType nodeType) {
        return NODES.get(nodeType);
    }

    public List<Node> getNodeList() {
        List<Node> nodes = new ArrayList<Node>();

        for (Map.Entry<NodeType, List<Node>> entry : NODES.entrySet()) {
            if (CollectionUtils.isNotEmpty(entry.getValue())) {
                nodes.addAll(entry.getValue());
            }
        }
        return nodes;
    }

    private void removeNode(Node delNode) {
        List<Node> nodeList = NODES.get(delNode.getNodeType());
        if (CollectionUtils.isNotEmpty(nodeList)) {
            for (Node node : nodeList) {
                if (node.getIdentity().equals(delNode.getIdentity())) {
                    nodeList.remove(node);
                    LOGGER.info("Remove {}", node);
                }
            }
        }
    }

}
