package com.bj58.zptask.dtplat.registry;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.registry.event.NodeAddEvent;
import com.bj58.zptask.dtplat.registry.event.NodeRemoveEvent;

/**
 * 平台注册是心接口
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月21日 下午4:41:10
 * @see 
 * @since
 */
public interface PlatRegistry {

    public static final Logger logger = Logger.getLogger(PlatRegistry.class);

    //zookeeper层面的节点注册
    void register(Node node) throws Exception;

    //zookeeper层面的节点删除
    void unregister(Node node) throws Exception;

    //系统层面的事件注册
    void subscribeChildChange(Node node, Object obj) throws Exception;

    //系统层面的事件删除
    void unsubscribeChildChange(Node node, Object obj) throws Exception;

    void childAdd(NodeAddEvent event) throws Exception;

    void childRemove(NodeRemoveEvent event) throws Exception;

    //销毁注册中心  释放资源
    void destroy() throws Exception;

    //恢复 每一个中心都有自已的恢复过程
    void recover() throws Exception;
}
