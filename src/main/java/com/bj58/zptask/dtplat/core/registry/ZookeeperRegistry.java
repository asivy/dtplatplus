package com.bj58.zptask.dtplat.core.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.zookeeper.ChildListener;
import com.bj58.zptask.dtplat.zookeeper.CuratorZKClient;
import com.bj58.zptask.dtplat.zookeeper.NodePathHelper;
import com.bj58.zptask.dtplat.zookeeper.StateListener;
import com.bj58.zptask.dtplat.zookeeper.ZookeeperClient;

/**
 * 基于ZK的注册中心  
 * 节点注册器，并监听自己关注的节点
 * 当自已的节点添加上去后  不仅要通知到别人  还要接收自已应有的通知
 * 因此注册中心是：ZK+订阅者模式   其里面的内容比纯ZK多了很多 
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private static final Logger logger = Logger.getLogger(ZookeeperRegistry.class);

    private ZookeeperClient zkClient;
    // 用来记录父节点下的子节点的变化
    private final ConcurrentHashMap<String/* parentPath */, List<String/* children */>> cachedChildrenNodeMap;

    private final ConcurrentMap<Node, ConcurrentMap<NotifyListener, ChildListener>> zkListeners;

    private String clusterName;

    public ZookeeperRegistry(Config config) {
        super(config);
        this.clusterName = config.getClusterName();
        this.cachedChildrenNodeMap = new ConcurrentHashMap<String, List<String>>();
        //原方式实现的很差 它使得在其它地方使用zkclient变得很困验
        //        ZookeeperTransporter zookeeperTransporter = InjectorHolder.getInstance(ZookeeperTransporter.class);
        //        this.zkClient = zookeeperTransporter.connect(config);
        this.zkClient = InjectorHolder.getInstance(CuratorZKClient.class);
        this.zkListeners = new ConcurrentHashMap<Node, ConcurrentMap<NotifyListener, ChildListener>>();
        zkClient.addStateListener(new StateListener() {
            @Override
            public void stateChanged(int state) {
                if (state == RECONNECTED) {
                    try {
                        recover();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    @Override
    protected void doRegister(Node node) {
        if (zkClient.exists(node.toFullString())) {
            return;
        }
        zkClient.create(node.toFullString(), true, false);
    }

    @Override
    protected void doUnRegister(Node node) {
        zkClient.delete(node.toFullString());
    }

    /**
     * 初次订阅时 触发 
     * 
     */
    @Override
    protected void doSubscribe(Node node, NotifyListener listener) {
        List<String> listenNodeTypes = node.getListenNodeTypes();
        if (CollectionUtils.isEmpty(listenNodeTypes)) {
            return;
        }
        for (String listenNodeType : listenNodeTypes) {
            String listenNodePath = NodePathHelper.getNodeTypePath(listenNodeType.toString());

            ChildListener zkListener = addZkListener(node, listener);
            
            // 为自己关注的 节点 添加监听 
            List<String> children = zkClient.addChildListener(listenNodePath, zkListener);

            //向此节点下的所有子节点通知变更
            if (CollectionUtils.isNotEmpty(children)) {
                List<Node> listenedNodes = new ArrayList<Node>();
                for (String child : children) {
                    Node listenedNode = NodePathHelper.parse(listenNodePath + "/" + child);
                    listenedNodes.add(listenedNode);
                }
                logger.info("event when dosubscribe");
                notify(NotifyEvent.ADD, listenedNodes, listener);
                cachedChildrenNodeMap.put(listenNodePath, children);
            }
        }
    }

    @Override
    protected void doUnsubscribe(Node node, NotifyListener listener) {
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(node);
        if (listeners != null) {
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
                List<String> listenNodeTypes = node.getListenNodeTypes();
                if (CollectionUtils.isEmpty(listenNodeTypes)) {
                    return;
                }
                for (String listenNodeType : listenNodeTypes) {
                    String listenNodePath = NodePathHelper.getNodeTypePath(listenNodeType);
                    zkClient.removeChildListener(listenNodePath, zkListener);
                }
            }
        }
    }

    private ChildListener addZkListener(Node node, final NotifyListener listener) {

        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(node);

        if (listeners == null) {
            zkListeners.putIfAbsent(node, new ConcurrentHashMap<NotifyListener, ChildListener>());
            listeners = zkListeners.get(node);
        }
        ChildListener zkListener = listeners.get(listener);
        if (zkListener == null) {

            listeners.putIfAbsent(listener, new ChildListener() {
                //匿名也有好处 不用把参数传来传去了 
                public void childChanged(String parentPath, List<String> currentChilds) {

                    if (CollectionUtils.isEmpty(currentChilds)) {
                        currentChilds = new ArrayList<String>(0);
                    }

                    List<String> oldChilds = cachedChildrenNodeMap.get(parentPath);
                    // 1. 找出增加的 节点
                    List<String> addChilds = CollectionUtils.getLeftDiff(currentChilds, oldChilds);
                    // 2. 找出减少的 节点
                    List<String> decChilds = CollectionUtils.getLeftDiff(oldChilds, currentChilds);

                    if (CollectionUtils.isNotEmpty(addChilds)) {

                        List<Node> nodes = new ArrayList<Node>(addChilds.size());
                        for (String child : addChilds) {
                            Node node = NodePathHelper.parse(parentPath + "/" + child);
                            nodes.add(node);
                        }
                        ZookeeperRegistry.this.notify(NotifyEvent.ADD, nodes, listener);
                    }

                    if (CollectionUtils.isNotEmpty(decChilds)) {
                        List<Node> nodes = new ArrayList<Node>(addChilds.size());
                        for (String child : decChilds) {
                            Node node = NodePathHelper.parse(parentPath + "/" + child);
                            nodes.add(node);
                        }
                        ZookeeperRegistry.this.notify(NotifyEvent.REMOVE, nodes, listener);
                    }
                    cachedChildrenNodeMap.put(parentPath, currentChilds);
                }
            });
            zkListener = listeners.get(listener);
        }
        return zkListener;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getNode() + ", cause: " + e.getMessage(), e);
        }
    }
}
