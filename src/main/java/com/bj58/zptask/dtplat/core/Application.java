package com.bj58.zptask.dtplat.core;

import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.MasterElector;
import com.bj58.zptask.dtplat.core.eventcenter.EventCenter;
import com.bj58.zptask.dtplat.core.protocol.command.CommandBodyWrapper;
import com.bj58.zptask.dtplat.registry.event.SubscribedNodeManager;

/**
 * 对应配置 数据 事件信息
 * @author Robert HG (254963746@qq.com) on 8/17/14. 用来存储 程序的数据
 */
public abstract class Application {

    // 节点配置信息
    private Config config;
    // 节点管理
    private SubscribedNodeManager subscribedNodeManager;
    // master节点选举者
    private MasterElector masterElector;
    // 节点通信CommandBody包装器
    private CommandBodyWrapper commandBodyWrapper;
    // 事件中心
    private EventCenter eventCenter;

    public EventCenter getEventCenter() {
        return eventCenter;
    }

    public void setEventCenter(EventCenter eventCenter) {
        this.eventCenter = eventCenter;
    }

    public CommandBodyWrapper getCommandBodyWrapper() {
        return commandBodyWrapper;
    }

    public void setCommandBodyWrapper(CommandBodyWrapper commandBodyWrapper) {
        this.commandBodyWrapper = commandBodyWrapper;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public SubscribedNodeManager getSubscribedNodeManager() {
        return subscribedNodeManager;
    }

    public void setSubscribedNodeManager(SubscribedNodeManager subscribedNodeManager) {
        this.subscribedNodeManager = subscribedNodeManager;
    }

    public MasterElector getMasterElector() {
        return masterElector;
    }

    public void setMasterElector(MasterElector masterElector) {
        this.masterElector = masterElector;
    }

}
