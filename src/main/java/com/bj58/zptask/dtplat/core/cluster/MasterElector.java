package com.bj58.zptask.dtplat.core.cluster;

import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.core.eventcenter.EventInfo;
import com.bj58.zptask.dtplat.core.listener.MasterChangeListener;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.EcTopic;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *  选出每种节点中得master, 通过每个节点的创建时间来决定 （创建时间最小的便是master, 即最早创建的是master）
 *  当master 挂掉之后, 要重新选举
 *  
 *  为什么不直接使用Curator的Master先举方式呢 
 *  这样 无故的把创建时间最小的 当做M 明显有很多问题啊  就因为支持Redis吗
 *  
 *  需要重写
 *  
 */
public class MasterElector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterElector.class);

    private Application application;
    private List<MasterChangeListener> listeners;
    private volatile Node master;

    public MasterElector(Application application) {
        this.application = application;
    }

    public void addMasterChangeListener(List<MasterChangeListener> masterChangeListeners) {
        if (listeners == null) {
            listeners = new CopyOnWriteArrayList<MasterChangeListener>();
        }
        if (CollectionUtils.isNotEmpty(masterChangeListeners)) {
            listeners.addAll(masterChangeListeners);
        }
    }

    public void addNodes(List<Node> nodes) {
        Node newMaster = null;
        for (Node node : nodes) {
            if (newMaster == null) {
                newMaster = node;
            } else {
                if (newMaster.getCreateTime() > node.getCreateTime()) {
                    newMaster = node;
                }
            }
        }
        addNode(newMaster);
    }

    /**
     * 这种判断方式也太LOW了吧
     * 当前节点是否是master
     */
    public boolean isCurrentMaster() {
        if (master != null && master.getIdentity().equals(application.getConfig().getIdentity())) {
            return true;
        }
        return false;
    }

    public void addNode(Node newNode) {
        if (master == null) {
            master = newNode;
            notifyListener();
        } else {
            if (master.getCreateTime() > newNode.getCreateTime()) {
                master = newNode;
                notifyListener();
            }
        }
    }

    public void removeNode(List<Node> removedNodes) {
        if (master != null) {
            boolean masterRemoved = false;
            for (Node removedNode : removedNodes) {
                if (master.getIdentity().equals(removedNode.getIdentity())) {
                    masterRemoved = true;
                }
            }
            if (masterRemoved) {
                // 如果挂掉的是master, 需要重新选举
                List<Node> nodes = application.getSubscribedNodeManager().getNodeList(application.getConfig().getNodeType(), application.getConfig().getNodeGroup());
                if (CollectionUtils.isNotEmpty(nodes)) {
                    Node newMaster = null;
                    for (Node node : nodes) {
                        if (newMaster == null) {
                            newMaster = node;
                        } else {
                            if (newMaster.getCreateTime() > node.getCreateTime()) {
                                newMaster = node;
                            }
                        }
                    }
                    master = newMaster;
                    notifyListener();
                }
            }
        }
    }

    private void notifyListener() {
        boolean isMaster = false;
        if (application.getConfig().getIdentity().equals(master.getIdentity())) {
            LOGGER.info("Current node become the master node:{}", master);
            isMaster = true;
        } else {
            LOGGER.info("Master node is :{}", master);
            isMaster = false;
        }

        if (listeners != null) {
            for (MasterChangeListener masterChangeListener : listeners) {
                try {
                    masterChangeListener.change(master, isMaster);
                } catch (Throwable t) {
                    LOGGER.error("MasterChangeListener notify error!", t);
                }
            }
        }
        EventInfo eventInfo = new EventInfo(EcTopic.MASTER_CHANGED);
        eventInfo.setParam("master", master);
        eventInfo.setParam("isMaster", isMaster);
        application.getEventCenter().publishSync(eventInfo);
    }

}
