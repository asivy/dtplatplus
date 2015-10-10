package com.bj58.zptask.dtplat.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.registry.event.NodeAddEvent;
import com.bj58.zptask.dtplat.registry.event.NodeRemoveEvent;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.util.GenericsUtils;
import com.bj58.zptask.dtplat.zookeeper.CuratorZKClient;
import com.bj58.zptask.dtplat.zookeeper.NodePathHelper;
import com.bj58.zptask.dtplat.zookeeper.StateListener;

/**
 * 底层zokeeper层的注册中心 
 * 
 * 这种以继承的方式来实现并好  继承是因为有承接包含关系  但现在是三个不同的模块了 
 * 任何一个模块出了问题  其它模块都要回滚的   
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月21日 下午4:42:54
 * @see 
 * @since
 */
public class CuratorPlateRegistry extends RecoveryPlatRegistry {

    public static final Logger logger = Logger.getLogger(CuratorPlateRegistry.class);

    private CuratorZKClient curatorClient;

    private final ConcurrentHashMap<String, PathChildrenCache> childCacheMap = GenericsUtils.newConcurrentHashMap();

    public CuratorPlateRegistry() {
        curatorClient = InjectorHolder.getInstance(CuratorZKClient.class);
        curatorClient.addStateListener(new StateListener() {
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
        logger.info(String.format("%sRegistry Center Init Success%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    //所有的路径信息  都来源于此
    @Override
    public void register(Node node) throws Exception {
        super.register(node);
        if (curatorClient.exists(node.toFullString())) {
            return;
        }
        //注册此节点
        curatorClient.create(node.toFullString(), true, false);
    }

    @Override
    public void unregister(Node node) throws Exception {
        super.unregister(node);
        curatorClient.delete(node.toFullString());
        PathChildrenCache cached = childCacheMap.get(node.toFullString());
        if (cached != null) {
            cached.clearAndRefresh();
            cached.close();
        }
    }

    /**
     * 订阅此节点想要关心的事件信
     * 如job节点一般只需要关心task节点进行channel的管理
     * task节点一般只需要关心job节点 
     */
    @Override
    public void subscribeChildChange(Node node, Object obj) throws Exception {
        super.subscribeChildChange(node, obj);

        List<String> listenNodeGroups = node.getListenNodeTypes();
        if (CollectionUtils.isEmpty(listenNodeGroups)) {
            return;
        }
        for (String group : listenNodeGroups) {
            String listenNodePath = NodePathHelper.getNodeTypePath(group);
            PathChildrenCache cached = childCacheMap.get(listenNodePath);
            if (cached == null) {
                //补偿子节点通知
                List<String> childen = curatorClient.getChildren(listenNodePath);
                if (childen != null && childen.size() > 0) {
                    for (String child : childen) {
                        NodeAddEvent addEvent = new NodeAddEvent();
                        addEvent.setPath(listenNodePath + "/" + child);
                        childAdd(addEvent);
                    }
                }
                //监听子节点
                cached = pathChildrenCacheListener(listenNodePath, true);
                childCacheMap.put(listenNodePath, cached);
            }
        }
    }

    @Override
    public void unsubscribeChildChange(Node node, Object obj) throws Exception {
        super.unsubscribeChildChange(node, obj);
    }

    /**
     * 增加节点变动事件监听
     * @param path
     * @param cacheData
     * @return
     * @throws Exception
     */
    public PathChildrenCache pathChildrenCacheListener(String path, Boolean cacheData) throws Exception {
        PathChildrenCache cached = childCacheMap.get(path);
        if (cached != null) {
            return cached;
        }
        cached = new PathChildrenCache(curatorClient.getClient(), path, cacheData);
        cached.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                PathChildrenCacheEvent.Type eventType = event.getType();
                switch (eventType) {
                    case CHILD_ADDED:
                        NodeAddEvent addEvent = new NodeAddEvent();
                        addEvent.setPath(event.getData().getPath());
                        logger.info(JSON.toJSONString(event));
                        childAdd(addEvent);
                        break;
                    case CHILD_REMOVED:
                        NodeRemoveEvent removeEvent = new NodeRemoveEvent();
                        removeEvent.setPath(event.getData().getPath());
                        logger.info(JSON.toJSONString(event));
                        childRemove(removeEvent);
                    default:
                        logger.info("PathChildrenCache changed : {path:" + event.getData().getPath() + " data:" + new String(event.getData().getData()) + "}");
                }
            }
        });
        childCacheMap.put(path, cached);
        cached.start();
        return cached;
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
        childCacheMap.clear();
        curatorClient.close();
    }

    @Override
    public void recover() throws Exception {
        super.recover();
    }

}
