package com.bj58.zptask.dtplat.core.cluster;

import java.util.concurrent.Executors;

import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.core.factory.NamedThreadFactory;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;
import com.bj58.zptask.dtplat.rpc.netty.NettyServerConfig;
import com.bj58.zptask.dtplat.util.Constants;

/**
 * 继承的层次有点太多了
 */
public abstract class AbstractServerNode<T extends Node, App extends Application> extends AbstractJobNode<T, App> {

    protected RemotingServerDelegate remotingServer;

    protected void remotingStart() {

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // config 配置
        nettyServerConfig.setListenPort(config.getListenPort());

        //        remotingServer = new RemotingServerDelegate(new NettyRemotingServer(nettyServerConfig), application);

        //        remotingServer.start();

        NettyRequestProcessor defaultProcessor = getDefaultProcessor();
        if (defaultProcessor != null) {
            int processorSize = config.getParameter(Constants.PROCESSOR_THREAD, Constants.DEFAULT_PROCESSOR_THREAD);
            remotingServer.registerDefaultProcessor(defaultProcessor, Executors.newFixedThreadPool(processorSize, new NamedThreadFactory(AbstractServerNode.class.getSimpleName())));
        }
        injectRemotingServer();
    }

    protected void injectRemotingServer() {

    }

    public void setListenPort(int listenPort) {
        config.setListenPort(listenPort);
    }

    protected void remotingStop() {
        remotingServer.shutdown();
    }

    /**
     * 得到默认的处理器
     *
     * @return
     */
    protected abstract NettyRequestProcessor getDefaultProcessor();

}
