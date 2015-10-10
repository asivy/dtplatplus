package com.bj58.zptask.dtplat.jobtracker.processor;

import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;

/**
 * @author Robert HG (254963746@qq.com) on 8/16/14.
 */
public abstract class AbstractProcessor implements NettyRequestProcessor {

    protected RemotingServerDelegate remotingServer;
    protected JobTrackerApplication application;

    public AbstractProcessor(RemotingServerDelegate remotingServer, JobTrackerApplication application) {
        this.remotingServer = remotingServer;
        this.application = application;
    }
    
}
