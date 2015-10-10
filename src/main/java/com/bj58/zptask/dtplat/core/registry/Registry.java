package com.bj58.zptask.dtplat.core.registry;

import com.bj58.zptask.dtplat.core.cluster.Node;

/**
 * 实现的一个注册中心 
 * 类监听者模式
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月9日 下午1:58:16
 * @see 
 * @since
 */
public interface Registry {

    /**
     * 节点注册
     *
     * @param node
     */
    void register(Node node);

    /**
     * 节点 取消注册
     *
     * @param node
     */
    void unregister(Node node);

    /**
     * 监听节点
     *
     * @param listener
     */
    void subscribe(Node node, NotifyListener listener);
    
    /**
     * 取消监听节点
     *
     * @param node
     * @param listener
     */
    void unsubscribe(Node node, NotifyListener listener);

    /**
     * 销毁
     */
    void destroy();
}
