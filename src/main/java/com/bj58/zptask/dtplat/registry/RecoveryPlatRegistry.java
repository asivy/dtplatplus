package com.bj58.zptask.dtplat.registry;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.factory.NamedThreadFactory;
import com.bj58.zptask.dtplat.registry.event.NodeAddEvent;
import com.bj58.zptask.dtplat.registry.event.NodeRemoveEvent;

/**
 * 针对事件的失败恢复中心 
 * 
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月21日 下午4:42:07
 * @see 
 * @since
 */
public abstract class RecoveryPlatRegistry extends EventPlatRegistry {

    public static final Logger logger = Logger.getLogger(RecoveryPlatRegistry.class);

    //定时任务
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("RecoveryScheduler", true));

    @Override
    public void register(Node node) throws Exception {
        super.register(node);
    }

    @Override
    public void unregister(Node node) throws Exception {
        super.unregister(node);
    }

    @Override
    public void subscribeChildChange(Node node, Object obj) throws Exception {
        super.subscribeChildChange(node, obj);
    }

    @Override
    public void unsubscribeChildChange(Node node, Object obj) throws Exception {
        super.unsubscribeChildChange(node, obj);
    }

    @Override
    public void childAdd(NodeAddEvent event) throws Exception {
        super.childAdd(event);
    }

    @Override
    public void childRemove(NodeRemoveEvent event) throws Exception {
        super.childRemove(event);
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
    }

    @Override
    public void recover() throws Exception {
        super.recover();
    }

}
