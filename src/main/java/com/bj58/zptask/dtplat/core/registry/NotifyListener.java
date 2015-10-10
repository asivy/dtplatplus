package com.bj58.zptask.dtplat.core.registry;

import com.bj58.zptask.dtplat.core.cluster.Node;

import java.util.List;

/**
 * 通知中心
 * 三个实现类  居然都是匿名内部类
 * 
 * 可以改为 eventbus的实现  
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月21日 下午3:51:43
 * @see 
 * @since
 */
public interface NotifyListener {
    void notify(NotifyEvent event, List<Node> nodes);
}
