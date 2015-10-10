//package com.bj58.zptask.dtplat.tasktracker;
//
//import com.bj58.zptask.dtplat.core.cluster.AbstractClientNode;
//import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;
//import com.bj58.zptask.dtplat.tasktracker.domain.TaskTrackerApplication;
//import com.bj58.zptask.dtplat.tasktracker.domain.TaskTrackerNode;
//import com.bj58.zptask.dtplat.tasktracker.processor.RemotingDispatcher;
//import com.bj58.zptask.dtplat.tasktracker.runner.JobRunner;
//import com.bj58.zptask.dtplat.tasktracker.runner.RunnerPool;
//import com.bj58.zptask.dtplat.tasktracker.support.JobPullMachine;
//import com.bj58.zptask.dtplat.util.Level;
//
///**
// *         任务执行节点
// */
//public class TaskTracker extends AbstractClientNode<TaskTrackerNode, TaskTrackerApplication> {
//
//    @Override
//    protected void innerStart() {
//        // 设置 线程池
//        application.setRunnerPool(new RunnerPool(application));
//        application.setJobPullMachine(new JobPullMachine(application));
//    }
//
//    @Override
//    protected void injectRemotingClient() {
//        application.setRemotingClient(remotingClient);
//    }
//
//    @Override
//    protected NettyRequestProcessor getDefaultProcessor() {
//        return new RemotingDispatcher(remotingClient, application);
//    }
//
//    public <JRC extends JobRunner> void setJobRunnerClass(Class<JRC> clazz) {
//        application.setJobRunnerClass(clazz);
//    }
//
//    public void setWorkThreads(int workThreads) {
//        config.setWorkThreads(workThreads);
//    }
//
//    /**
//     * 设置业务日志记录级别
//     *
//     * @param level
//     */
//    public void setBizLoggerLevel(Level level) {
//        if (level != null) {
//            application.setBizLogLevel(level);
//        }
//    }
//}
