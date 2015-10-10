package com.bj58.zptask.dtplat.core.cluster;

/**
 * 此为方法接口类 此类好多余
 * @author Robert HG (254963746@qq.com) on 8/14/14. 节点接口
 */
public interface JobNode {

    /**
     * 启动节点
     */
    public void start();

    /**
     * 停止节点
     */
    public void stop();

    /**
     * destroy
     */
    public void destroy();
}
