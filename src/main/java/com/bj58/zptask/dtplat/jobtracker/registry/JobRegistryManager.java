package com.bj58.zptask.dtplat.jobtracker.registry;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.utils.CloseableUtils;
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
import com.bj58.zptask.dtplat.jobtracker.Damon;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerNode;
import com.bj58.zptask.dtplat.registry.CuratorPlateRegistry;
import com.bj58.zptask.dtplat.registry.event.SubscribedNodeManager;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.zookeeper.CuratorZKClient;
import com.google.common.eventbus.EventBus;
import com.google.inject.Singleton;

/**
 * 管理server端的节点变更 和 Master变更
 * 
 * 
 * 原JOB订阅了  感觉好重啊
 * MasterChangeListenerImpl
 * JobNodeChangeListener   以组的形式  管理task 和 client  并做一些检查性工作    只有job加了
 * JobTrackerMasterChangeListener
 * SubscribedNodeManager   以类型的形式  对所有的节点进行管理 
 * MasterElectionListener
 * SelfChangeListener
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午7:25:19
 * @see 
 * @since
 */
@Singleton
public class JobRegistryManager implements Damon {

    private static final Logger logger = Logger.getLogger(JobRegistryManager.class);

    protected JobTrackerNode node;
    protected Config config;
    protected CuratorPlateRegistry registry;
    protected JobMasterElector elector;
    protected EventBus nodeChangeEventBus;//事件通知类

    public JobRegistryManager() {
        config = InjectorHolder.getInstance(Config.class);
        node = NodeFactory.create(JobTrackerNode.class, config);
        registry = new CuratorPlateRegistry();
        elector = new JobMasterElector(InjectorHolder.getInstance(CuratorZKClient.class).getClient(), "JOB");
    }

    @Override
    public void start() throws Exception {
        registry.register(node);
        elector.start();
        registry.subscribeChildChange(node, new TaskNodeChangeBus());
        //        registry.subscribeChange(node, new SubscribedNodeManager());
        //        registry.subscribeChange(node, new SelfChangeListener());
        logger.info(String.format("%sJob Registry Init Success%s ", Constants.LOGTIP, Constants.LOGTIP));
    }

    @Override
    public void stop() throws Exception {
        if (registry != null) {
            registry.unregister(node);
            registry.destroy();
        }
        CloseableUtils.closeQuietly(elector);
    }

}
