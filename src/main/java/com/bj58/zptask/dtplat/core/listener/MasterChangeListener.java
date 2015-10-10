package com.bj58.zptask.dtplat.core.listener;

import com.bj58.zptask.dtplat.core.cluster.Node;

/**
 * 
 */
public interface MasterChangeListener {

    /**
     * master 为 master节点 isMaster 表示当前节点是不是master节点
     * 
     * @param master
     * @param isMaster
     */
    public void change(Node master, boolean isMaster);

}
