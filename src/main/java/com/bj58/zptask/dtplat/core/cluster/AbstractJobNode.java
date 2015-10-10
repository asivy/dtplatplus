package com.bj58.zptask.dtplat.core.cluster;

import java.util.ArrayList;
import java.util.List;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.core.eventcenter.EventCenterFactory;
import com.bj58.zptask.dtplat.core.factory.NodeFactory;
import com.bj58.zptask.dtplat.core.listener.MasterChangeListener;
import com.bj58.zptask.dtplat.core.listener.MasterElectionListener;
import com.bj58.zptask.dtplat.core.listener.NodeChangeListener;
import com.bj58.zptask.dtplat.core.listener.SelfChangeListener;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.core.protocol.command.CommandBodyWrapper;
import com.bj58.zptask.dtplat.core.registry.AbstractRegistry;
import com.bj58.zptask.dtplat.core.registry.NotifyEvent;
import com.bj58.zptask.dtplat.core.registry.NotifyListener;
import com.bj58.zptask.dtplat.core.registry.Registry;
import com.bj58.zptask.dtplat.core.registry.ZookeeperRegistry;
import com.bj58.zptask.dtplat.registry.event.SubscribedNodeManager;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.GenericsUtils;

/**
 *  抽象节点
 *  对节点的管理 是监听的ZK的节点的变化
 */
public abstract class AbstractJobNode<T extends Node, App extends Application> implements JobNode {

    protected static final Logger LOGGER = LoggerFactory.getLogger(JobNode.class);
    //    protected static final Logger LOGGER = Logge

    protected Registry registry;
    protected T node;
    protected Config config;
    protected App application;
    private List<NodeChangeListener> nodeChangeListeners;//监听ZK节点变化
    private List<MasterChangeListener> masterChangeListeners;//监听ZK MASTER变化

    //    private EventCenterFactory eventCenterFactory = ExtensionLoader.getExtensionLoader(EventCenterFactory.class).getAdaptiveExtension();

    public AbstractJobNode() {
        application = getApplication();
        config = InjectorHolder.getInstance(Config.class);
        application.setConfig(config);
        nodeChangeListeners = new ArrayList<NodeChangeListener>();
        masterChangeListeners = new ArrayList<MasterChangeListener>();
    }

    final public void start() {
        try {
            // 初始化配置
            initConfig();
            innerStart();
            remotingStart();
            initRegistry();
            registry.register(node);
            LOGGER.info("Start success!");
        } catch (Throwable e) {
            LOGGER.error("Start failed!", e);
        }
    }

    final public void stop() {
        try {
            registry.unregister(node);

            innerStop();
            remotingStop();

            LOGGER.info("Stop success!");
        } catch (Throwable e) {
            LOGGER.error("Stop failed!", e);
        }
    }

    @Override
    public void destroy() {
        try {
            registry.destroy();
            LOGGER.info("Destroy success!");
        } catch (Throwable e) {
            LOGGER.error("Destroy failed!", e);
        }
    }

    protected void initConfig() {
        application.setCommandBodyWrapper(new CommandBodyWrapper(config));
        application.setMasterElector(new MasterElector(application));
        application.getMasterElector().addMasterChangeListener(masterChangeListeners);

        node = NodeFactory.create(getNodeClass(), config);
        config.setNodeType(node.getNodeType());

        LOGGER.info("Current node config :{}", config);

        application.setEventCenter(InjectorHolder.getInstance(EventCenterFactory.class).getEventCenter(config));

        // 订阅的node管理
        SubscribedNodeManager subscribedNodeManager = new SubscribedNodeManager();
        application.setSubscribedNodeManager(subscribedNodeManager);
        //        nodeChangeListeners.add(subscribedNodeManager);
        // 用于master选举的监听器
        nodeChangeListeners.add(new MasterElectionListener(application));
        // 监听自己节点变化（如，当前节点被禁用了）
        nodeChangeListeners.add(new SelfChangeListener());
    }

    private void initRegistry() {
        registry = new ZookeeperRegistry(config);
        if (registry instanceof AbstractRegistry) {
            ((AbstractRegistry) registry).setNode(node);
        }
        registry.subscribe(node, new NotifyListener() {
            private final Logger NOTIFY_LOGGER = LoggerFactory.getLogger(NotifyListener.class);

            @Override
            public void notify(NotifyEvent event, List<Node> nodes) {
                if (CollectionUtils.isEmpty(nodes)) {
                    return;
                }
                System.out.println("job receive node notify ");
                for (Node n : nodes) {
                    System.out.println("---" + n.toFullString());
                }
                switch (event) {
                    case ADD:
                        for (NodeChangeListener listener : nodeChangeListeners) {
                            try {
                                listener.addNodes(nodes);
                            } catch (Throwable t) {
                                NOTIFY_LOGGER.error("{} add nodes failed , cause: {}", listener.getClass(), t.getMessage(), t);
                            }
                        }
                        break;
                    case REMOVE:
                        for (NodeChangeListener listener : nodeChangeListeners) {
                            try {
                                listener.removeNodes(nodes);
                            } catch (Throwable t) {
                                NOTIFY_LOGGER.error("{} remove nodes failed , cause: {}", listener.getClass(), t.getMessage(), t);
                            }
                        }
                        break;
                }
            }
        });
    }

    protected abstract void remotingStart();

    protected abstract void remotingStop();

    protected void innerStart() {
    }

    protected void innerStop() {
    }

    @SuppressWarnings("unchecked")
    private App getApplication() {
        try {
            return ((Class<App>) GenericsUtils.getSuperClassGenericType(this.getClass(), 1)).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Class<T> getNodeClass() {
        return (Class<T>) GenericsUtils.getSuperClassGenericType(this.getClass(), 0);
    }

    /**
     * 设置zookeeper注册中心地址
     *
     * @param registryAddress
     */
    public void setRegistryAddress(String registryAddress) {
        config.setRegistryAddress(registryAddress);
    }

    /**
     * 设置远程调用超时时间
     *
     * @param invokeTimeoutMillis
     */
    public void setInvokeTimeoutMillis(int invokeTimeoutMillis) {
        config.setInvokeTimeoutMillis(invokeTimeoutMillis);
    }

    /**
     * 设置集群名字
     *
     * @param clusterName
     */
    public void setClusterName(String clusterName) {
        config.setClusterName(clusterName);
    }

    public void setIdentity(String identity) {
        config.setIdentity(identity);
    }

    /**
     * 添加节点监听器
     *
     * @param notifyListener
     */
    public void addNodeChangeListener(NodeChangeListener notifyListener) {
        if (notifyListener != null) {
            nodeChangeListeners.add(notifyListener);
        }
    }

    /**
     * 添加 master 节点变化监听器
     *
     * @param masterChangeListener
     */
    public void addMasterChangeListener(MasterChangeListener masterChangeListener) {
        if (masterChangeListener != null) {
            masterChangeListeners.add(masterChangeListener);
        }
    }

    /**
     * 设置额外的配置参数
     *
     * @param key
     * @param value
     */
    public void addConfig(String key, String value) {
        config.setParameter(key, value);
    }
}
