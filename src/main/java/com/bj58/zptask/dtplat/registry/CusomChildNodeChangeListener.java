package com.bj58.zptask.dtplat.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

/**
 * 监听子节点的变化
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月21日 下午8:19:22
 * @see 
 * @since
 */
public class CusomChildNodeChangeListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        PathChildrenCacheEvent.Type eventType = event.getType();
        switch (eventType) {
            case CONNECTION_RECONNECTED:
                break;
            case CONNECTION_SUSPENDED:
            case CONNECTION_LOST:
                System.out.println("Connection error,waiting...");
                break;
            case CHILD_ADDED:

                break;
            case CHILD_REMOVED:

            default:
                System.out.println("PathChildrenCache changed : {path:" + event.getData().getPath() + " data:" + new String(event.getData().getData()) + "}");
        }
    }

}
