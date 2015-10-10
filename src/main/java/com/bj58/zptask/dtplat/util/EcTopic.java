package com.bj58.zptask.dtplat.util;

/**
 * @author Robert HG (254963746@qq.com) on 5/11/15.
 */
public interface EcTopic {

    // 工作线程变化
    String WORK_THREAD_CHANGE = "WORK_THREAD_CHANGE";
    // 节点启用
    String NODE_ENABLE = "NODE_ENABLE";
    // 节点禁用
    String NODE_DISABLE = "NODE_DISABLE";

    // 没有可用的JobTracker了
    String NO_JOB_TRACKER_AVAILABLE = "NO_JOB_TRACKER_AVAILABLE";
    // 有可用的JobTracker了
    String JOB_TRACKER_AVAILABLE = "JOB_TRACKER_AVAILABLE";
    // master 节点改变了
    String MASTER_CHANGED = "MASTER_CHANGED";
}
