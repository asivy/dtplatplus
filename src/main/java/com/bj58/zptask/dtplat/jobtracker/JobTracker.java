package com.bj58.zptask.dtplat.jobtracker;

import com.bj58.zptask.dtplat.core.cluster.AbstractServerNode;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelManager;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerNode;
import com.bj58.zptask.dtplat.jobtracker.processor.RemotingDispatcher;
import com.bj58.zptask.dtplat.jobtracker.support.cluster.JobClientManager;
import com.bj58.zptask.dtplat.jobtracker.support.cluster.TaskTrackerManager;
import com.bj58.zptask.dtplat.jobtracker.support.listener.JobNodeChangeListener;
import com.bj58.zptask.dtplat.jobtracker.support.listener.JobTrackerMasterChangeListener;
import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;

/**
 *  
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午1:51:13
 * @see 
 * @since
 */
public class JobTracker extends AbstractServerNode<JobTrackerNode, JobTrackerApplication> {

    //    private JobLoggerFactory jobLoggerFactory = ExtensionLoader.getExtensionLoader(JobLoggerFactory.class).getAdaptiveExtension();

    //    private CronJobQueueFactory cronJobQueueFactory = ExtensionLoader.getExtensionLoader(CronJobQueueFactory.class).getAdaptiveExtension();
    //    private ExecutableJobQueueFactory executableJobQueueFactory = ExtensionLoader.getExtensionLoader(ExecutableJobQueueFactory.class).getAdaptiveExtension();
    //    private ExecutingJobQueueFactory executingJobQueueFactory = ExtensionLoader.getExtensionLoader(ExecutingJobQueueFactory.class).getAdaptiveExtension();
    //    private JobFeedbackQueueFactory jobFeedbackQueueFactory = ExtensionLoader.getExtensionLoader(JobFeedbackQueueFactory.class).getAdaptiveExtension();
    //    private NodeGroupStoreFactory nodeGroupStoreFactory = ExtensionLoader.getExtensionLoader(NodeGroupStoreFactory.class).getAdaptiveExtension();

    public JobTracker() {
        // 添加节点变化监听器
        addNodeChangeListener(new JobNodeChangeListener(application));
        // channel 管理者
        application.setChannelManager(new ChannelManager());
        // JobClient 管理者
        application.setJobClientManager(new JobClientManager(application));
        // TaskTracker 管理者
        application.setTaskTrackerManager(new TaskTrackerManager(application));
        // 添加master节点变化监听器
        addMasterChangeListener(new JobTrackerMasterChangeListener(application));
    }

    //初始化各种服务层  由原来的扩展工厂模式改为注入模式
    @Override
    protected void innerStart() { //        application.setJobLogger(InjectorHolder.getInstance(MysqlJobLoggerFactory.class).getJobLogger(config));
        //        application.setExecutableJobQueue(InjectorHolder.getInstance(ExecutableJobQueueFactory.class).getQueue(config));
        //        application.setExecutingJobQueue(InjectorHolder.getInstance(ExecutingJobQueueFactory.class).getQueue(config));
        //        application.setCronJobQueue(InjectorHolder.getInstance(CronJobQueueFactory.class).getQueue(config));
        //        application.setJobFeedbackQueue(InjectorHolder.getInstance(JobFeedbackQueueFactory.class).getQueue(config));
        //        application.setNodeGroupStore(InjectorHolder.getInstance(NodeGroupStoreFactory.class).getStore(config));
        application.getChannelManager().start();
    }

    @Override
    protected void injectRemotingServer() {
        application.setRemotingServer(remotingServer);
    }

    @Override
    protected void innerStop() {
        application.getChannelManager().stop();
    }

    @Override
    protected NettyRequestProcessor getDefaultProcessor() {
        return new RemotingDispatcher(remotingServer, application);
    }

    /**
     * 设置反馈数据给JobClient的负载均衡算法
     * 现在已经不需要jobclient了 
     * @param loadBalance
     */
    public void setLoadBalance(String loadBalance) {
        config.setParameter("loadbalance", loadBalance);
    }

    //    public void setOldDataHandler(OldDataHandler oldDataHandler) {
    //        application.setOldDataHandler(oldDataHandler);
    //    }

}
