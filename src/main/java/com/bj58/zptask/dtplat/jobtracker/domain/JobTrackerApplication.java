package com.bj58.zptask.dtplat.jobtracker.domain;

import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelManager;
import com.bj58.zptask.dtplat.jobtracker.support.checker.ExecutingDeadJobChecker;
import com.bj58.zptask.dtplat.jobtracker.support.cluster.JobClientManager;
import com.bj58.zptask.dtplat.jobtracker.support.cluster.TaskTrackerManager;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;

/**
 * JobTracker Application
 *
 * @author Robert HG (254963746@qq.com) on 3/30/15.
 */
public class JobTrackerApplication extends Application {

    private RemotingServerDelegate remotingServer;
    // channel manager
    private ChannelManager channelManager;
    // JobClient manager for job tracker
    private JobClientManager jobClientManager;
    // TaskTracker manager for job tracker
    private TaskTrackerManager taskTrackerManager;
    // dead job checker
    private ExecutingDeadJobChecker executingDeadJobChecker;

    // old data handler, dirty data
    //    private OldDataHandler oldDataHandler;

    // biz logger
    //    private JobLogger jobLogger;
    //    private MysqlJobLogger jobLogger;

    // executable job queue（waiting for exec）
    //    private ExecutableJobQueue executableJobQueue;
    // executing job queue
    //    private ExecutingJobQueue executingJobQueue;
    // store the connected node groups
    //    private NodeGroupStore nodeGroupStore;

    // Cron Job queue
    //    private CronJobQueue cronJobQueue;
    // feedback queue
    //    private JobFeedbackQueue jobFeedbackQueue;
    // job id generator
    //    private IdGenerator idGenerator;

    //    public JobLogger getJobLogger() {
    //        return jobLogger;
    //    }
    //
    //    public void setJobLogger(JobLogger jobLogger) {
    //        this.jobLogger = jobLogger;
    //    }

    //    public JobFeedbackQueue getJobFeedbackQueue() {
    //        return jobFeedbackQueue;
    //    }

    //    public MysqlJobLogger getJobLogger() {
    //        return jobLogger;
    //    }
    //
    //    public void setJobLogger(MysqlJobLogger jobLogger) {
    //        this.jobLogger = jobLogger;
    //    }

    //    public void setJobFeedbackQueue(JobFeedbackQueue jobFeedbackQueue) {
    //        this.jobFeedbackQueue = jobFeedbackQueue;
    //    }

    public RemotingServerDelegate getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServerDelegate remotingServer) {
        this.remotingServer = remotingServer;
    }

    public ChannelManager getChannelManager() {
        return channelManager;
    }

    public void setChannelManager(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    public JobClientManager getJobClientManager() {
        return jobClientManager;
    }

    public void setJobClientManager(JobClientManager jobClientManager) {
        this.jobClientManager = jobClientManager;
    }

    public TaskTrackerManager getTaskTrackerManager() {
        return taskTrackerManager;
    }

    public void setTaskTrackerManager(TaskTrackerManager taskTrackerManager) {
        this.taskTrackerManager = taskTrackerManager;
    }

    public ExecutingDeadJobChecker getExecutingDeadJobChecker() {
        return executingDeadJobChecker;
    }

    public void setExecutingDeadJobChecker(ExecutingDeadJobChecker executingDeadJobChecker) {
        this.executingDeadJobChecker = executingDeadJobChecker;
    }

    //    public OldDataHandler getOldDataHandler() {
    //        return oldDataHandler;
    //    }
    //
    //    public void setOldDataHandler(OldDataHandler oldDataHandler) {
    //        this.oldDataHandler = oldDataHandler;
    //    }

    //    public CronJobQueue getCronJobQueue() {
    //        return cronJobQueue;
    //    }
    //
    //    public void setCronJobQueue(CronJobQueue cronJobQueue) {
    //        this.cronJobQueue = cronJobQueue;
    //    }

    //    public IdGenerator getIdGenerator() {
    //        return idGenerator;
    //    }
    //
    //    public void setIdGenerator(IdGenerator idGenerator) {
    //        this.idGenerator = idGenerator;
    //    }

    //    public ExecutableJobQueue getExecutableJobQueue() {
    //        return executableJobQueue;
    //    }
    //
    //    public void setExecutableJobQueue(ExecutableJobQueue executableJobQueue) {
    //        this.executableJobQueue = executableJobQueue;
    //    }

    //    public ExecutingJobQueue getExecutingJobQueue() {
    //        return executingJobQueue;
    //    }
    //
    //    public void setExecutingJobQueue(ExecutingJobQueue executingJobQueue) {
    //        this.executingJobQueue = executingJobQueue;
    //    }

    //    public NodeGroupStore getNodeGroupStore() {
    //        return nodeGroupStore;
    //    }
    //
    //    public void setNodeGroupStore(NodeGroupStore nodeGroupStore) {
    //        this.nodeGroupStore = nodeGroupStore;
    //    }
}
