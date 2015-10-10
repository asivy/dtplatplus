package com.bj58.zptask.dtplat.test.example.support;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.listener.MasterChangeListener;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 * 仅有这一个肯定是不够的 
 * 
 * @author Robert HG (254963746@qq.com) on 3/6/15.
 */
public class MasterChangeListenerImpl implements MasterChangeListener {

    /**
     * master 为 master节点
     * isMaster 表示当前节点是不是master节点
     *
     * @param master
     * @param isMaster
     */
    @Override
    public void change(Node master, boolean isMaster) {

        // 一个节点组master节点变化后的处理 , 譬如我多个JobClient， 但是有些事情只想只有一个节点能做。
        if (isMaster) {
            System.out.println(master.getIdentity() + " 成为了Master节点 ");
        } else {
            System.out.println(StringUtils.format("master节点变成了{}", master));
        }
    }
}
