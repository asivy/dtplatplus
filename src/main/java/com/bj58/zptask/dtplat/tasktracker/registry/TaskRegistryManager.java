package com.bj58.zptask.dtplat.tasktracker.registry;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.factory.NodeFactory;
import com.bj58.zptask.dtplat.core.listener.NodeChangeListener;
import com.bj58.zptask.dtplat.core.listener.SelfChangeListener;
import com.bj58.zptask.dtplat.core.registry.AbstractRegistry;
import com.bj58.zptask.dtplat.core.registry.NotifyEvent;
import com.bj58.zptask.dtplat.core.registry.NotifyListener;
import com.bj58.zptask.dtplat.core.registry.Registry;
import com.bj58.zptask.dtplat.core.registry.ZookeeperRegistry;
import com.bj58.zptask.dtplat.jobtracker.Damon;
import com.bj58.zptask.dtplat.registry.event.SubscribedNodeManager;
import com.bj58.zptask.dtplat.tasktracker.domain.TaskTrackerNode;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.google.common.eventbus.EventBus;
import com.google.inject.Singleton;

/**
 * Task的注删监听类
 * 监听JOB节点的变化
 * 监听同TaskGroup节点的变化
 * 同组中 选出一个做Master
 * 
 * Task只需要关注
 * 
 * 
 * 原来的监听：
 * MasterChangeListenerImpl
 * SubscribedNodeManager
 * MasterElectionListener
 * SelfChangeListener
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月18日 下午3:24:47
 * @see 
 * @since
 */
@Singleton
public class TaskRegistryManager implements Damon {

    private static final Logger logger = Logger.getLogger(TaskRegistryManager.class);

    protected TaskTrackerNode node;
    protected Config config;
    protected Registry registry;

    protected EventBus nodeChangeEventBus;//事件订阅总线   目前还没有任何订阅

    //监听ZK节点变化   只监听JOB节点变化  对同组的TASK不需要监听   只由Elector来改变就可以了
    private List<NodeChangeListener> nodeChangeListeners = new ArrayList<NodeChangeListener>();

    public TaskRegistryManager() {
        config = InjectorHolder.getInstance(Config.class);
        node = NodeFactory.create(TaskTrackerNode.class, config);
        registry = new ZookeeperRegistry(config);
        //        nodeChangeListeners.add(new SubscribedNodeManager());
        nodeChangeListeners.add(new SelfChangeListener());
        nodeChangeEventBus = new EventBus();
    }

    @Override
    public void start() throws Exception {
        initRegistry();
        registry.register(node);
    }

    @Override
    public void stop() throws Exception {
        if (registry != null) {
            registry.unregister(node);
            registry.destroy();
        }
    }

    private void initRegistry() {
        if (registry instanceof AbstractRegistry) {
            ((AbstractRegistry) registry).setNode(node);
        }
        registry.subscribe(node, new NotifyListener() {
            @Override
            public void notify(NotifyEvent event, List<Node> nodes) {
                if (CollectionUtils.isEmpty(nodes)) {
                    return;
                }
                for (Node n : nodes) {
                    System.out.println(n.toFullString());
                }
                logger.info("task receive node notify");
                switch (event) {
                    case ADD:
                        for (NodeChangeListener listener : nodeChangeListeners) {
                            try {
                                listener.addNodes(nodes);
                            } catch (Throwable t) {
                                t.printStackTrace();
                                logger.error(t);
                            }
                        }
                        break;
                    case REMOVE:
                        for (NodeChangeListener listener : nodeChangeListeners) {
                            try {
                                listener.removeNodes(nodes);
                            } catch (Throwable t) {
                                t.printStackTrace();
                                logger.error(t);
                            }
                        }
                        break;
                }
            }
        });
    }
}
