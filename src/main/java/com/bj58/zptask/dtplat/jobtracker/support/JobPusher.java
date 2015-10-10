package com.bj58.zptask.dtplat.jobtracker.support;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.entity.TaskExecutingBean;
import com.bj58.zhaopin.feature.entity.TaskLogBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.domain.JobPo;
import com.bj58.zptask.dtplat.core.factory.NamedThreadFactory;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.core.protocol.JobProtos;
import com.bj58.zptask.dtplat.core.protocol.command.JobPullRequest;
import com.bj58.zptask.dtplat.core.protocol.command.JobPushRequest;
import com.bj58.zptask.dtplat.exception.DuplicateJobException;
import com.bj58.zptask.dtplat.exception.RemotingSendException;
import com.bj58.zptask.dtplat.exception.RequestTimeoutException;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.jobtracker.domain.TaskTrackerNode;
import com.bj58.zptask.dtplat.rpc.InvokeCallback;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.netty.ResponseFuture;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.util.Holder;

/**
 *         任务分发管理
 * 1 启动线程
 * 2 检查TASK节点还有没有可用的线程数；
 * 3 有的话  则主动去推送消息给TASK；
 * 4 最后用 remotingServer.invokeAsync 去主动回调 ；一次通信变成了两次 感觉很没必要
 */
public class JobPusher {

    private final Logger LOGGER = LoggerFactory.getLogger(JobPusher.class);
    private JobTrackerApplication application;
    private final ExecutorService executorService;

    public JobPusher(JobTrackerApplication application) {
        this.application = application;
        this.executorService = Executors.newFixedThreadPool(Constants.AVAILABLE_PROCESSOR * 5, new NamedThreadFactory(JobPusher.class.getSimpleName()));
    }

    public void push(final RemotingServerDelegate remotingServer, final JobPullRequest request) {

        this.executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String nodeGroup = request.getNodeGroup();
                    String identity = request.getIdentity();
                    // 更新TaskTracker的可用线程数
                    application.getTaskTrackerManager().updateTaskTrackerAvailableThreads(nodeGroup, identity, request.getAvailableThreads(), request.getTimestamp());

                    TaskTrackerNode taskTrackerNode = application.getTaskTrackerManager().getTaskTrackerNode(nodeGroup, identity);

                    if (taskTrackerNode == null) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("taskTrackerNodeGroup:{}, taskTrackerIdentity:{} , didn't have node.", nodeGroup, identity);
                        }
                        return;
                    }

                    int availableThreads = taskTrackerNode.getAvailableThread().get();
                    if (availableThreads == 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("taskTrackerNodeGroup:{}, taskTrackerIdentity:{} , availableThreads:0", nodeGroup, identity);
                        }
                    }
                    
                    while (availableThreads > 0) {
                        //                        System.out.println("task availableThreads is" + availableThreads);
                        LOGGER.debug("taskTrackerNodeGroup:{}, taskTrackerIdentity:{} , availableThreads:{}", nodeGroup, identity, availableThreads);
                        // 推送任务
                        PushResult result = sendJob(remotingServer, taskTrackerNode);
                        if (result == PushResult.SUCCESS) {
                            availableThreads = taskTrackerNode.getAvailableThread().decrementAndGet();
                        } else {
                            break;
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Job push failed!", e);
                }
            }
        });
    }

    private enum PushResult {
        NO_JOB, // 没有任务可执行
        SUCCESS, //推送成功
        FAILED //推送失败
    }
    
    /**
     * 是否推送成功
     * 业务好重
     * @param remotingServer
     * @param taskTrackerNode
     * @return
     */
    private PushResult sendJob(RemotingServerDelegate remotingServer, TaskTrackerNode taskTrackerNode) {
        final String nodeGroup = taskTrackerNode.getNodeGroup();
        final String identity = taskTrackerNode.getIdentity();
        //        System.out.println("jobpusher send job " + nodeGroup + " " + identity);

        // 从mongo 中取一个可运行的job
        //        final JobPo jobPo = application.getExecutableJobQueue().take(nodeGroup, identity);
        TaskExecutableBean taskExecutable = InjectorHolder.getInstance(DTaskProvider.class).takeExecutaleTask(nodeGroup, identity);
        if (taskExecutable == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Job push failed: no job! nodeGroup=" + nodeGroup + ", identity=" + identity);
            }
            return PushResult.NO_JOB;
        }
        final JobPo jobPo = JobDomainConverter.convertTaskExecutableToJobpo(taskExecutable);

        JobPushRequest body = application.getCommandBodyWrapper().wrapper(new JobPushRequest());
        body.setJobWrapper(JobDomainConverter.convert(jobPo));
        RemotingCommand commandRequest = RemotingCommand.createRequestCommand(JobProtos.RequestCode.PUSH_JOB.code(), body);

        // 是否分发推送任务成功
        final Holder<Boolean> pushSuccess = new Holder<Boolean>(false);

        final CountDownLatch latch = new CountDownLatch(1);
        try {
            remotingServer.invokeAsync(taskTrackerNode.getChannel().getChannel(), commandRequest, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    try {
                        RemotingCommand responseCommand = responseFuture.getResponseCommand();
                        if (responseCommand == null) {
                            LOGGER.warn("Job push failed! response command is null!");
                            return;
                        }
                        if (responseCommand.getCode() == JobProtos.ResponseCode.JOB_PUSH_SUCCESS.code()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Job push success! nodeGroup=" + nodeGroup + ", identity=" + identity + ", job=" + jobPo);
                            }
                            pushSuccess.set(true);
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });

        } catch (RemotingSendException e) {
            LOGGER.error(e.getMessage(), e);
        }

        try {
            latch.await(Constants.LATCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RequestTimeoutException(e);
        }
        
        if (!pushSuccess.get()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Job push failed! nodeGroup=" + nodeGroup + ", identity=" + identity + ", job=" + jobPo);
            }
            //            application.getExecutableJobQueue().resume(jobPo);
            System.out.println("bpusher reset executable");
            InjectorHolder.getInstance(DTaskProvider.class).resetExecutableTask(Long.parseLong(jobPo.getJobId()), jobPo.getTaskTrackerNodeGroup());
            return PushResult.FAILED;
        }
        try {
            //            application.getExecutingJobQueue().add(jobPo);
            TaskExecutingBean taskExecuting = JobDomainConverter.convertjobPotoTaskExecuting(jobPo);
            InjectorHolder.getInstance(DTaskProvider.class).addExecutingTask(taskExecuting);
        } catch (DuplicateJobException e) {
            // ignore
        }
        System.out.println("delete this executable" + JSON.toJSONString(jobPo));
        //        application.getExecutableJobQueue().remove(jobPo.getTaskTrackerNodeGroup(), jobPo.getJobId());
        InjectorHolder.getInstance(DTaskProvider.class).deleteExecutableTask(Long.parseLong(jobPo.getJobId()), jobPo.getTaskTrackerNodeGroup());
        
        // 记录日志

        //        JobLogPo jobLogPo = JobDomainConverter.convertJobLog(jobPo);
        //        jobLogPo.setSuccess(true);
        //        jobLogPo.setLogType(LogType.SENT);
        //        jobLogPo.setLogTime(SystemClock.now());
        //        jobLogPo.setLevel(Level.INFO);
        //        application.getJobLogger().log(jobLogPo);
        TaskLogBean entity = JobDomainConverter.convertJobToTaskLog(jobPo);
        entity.setSuccess(true);
        entity.setLogType("SEND");
        entity.setCreateDate(new Date());
        entity.setLevel("INFO");
        //打下日志
        InjectorHolder.getInstance(DTaskProvider.class).addTaskLog(entity);
        return PushResult.SUCCESS;
    }

}
