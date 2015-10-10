package com.bj58.zptask.dtplat.jobtracker.support;

import com.bj58.zptask.dtplat.core.domain.Action;
import com.bj58.zptask.dtplat.core.domain.JobResult;
import com.bj58.zptask.dtplat.core.domain.TaskTrackerJobResult;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.core.protocol.JobProtos;
import com.bj58.zptask.dtplat.core.protocol.command.JobFinishedRequest;
import com.bj58.zptask.dtplat.exception.RemotingSendException;
import com.bj58.zptask.dtplat.exception.RequestTimeoutException;
import com.bj58.zptask.dtplat.jobtracker.domain.JobClientNode;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.rpc.InvokeCallback;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.netty.ResponseFuture;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.util.Holder;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月17日 上午10:20:59
 * @see 
 * @since
 */
public class ClientNotifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientNotifier.class.getSimpleName());
    private ClientNotifyHandler clientNotifyHandler;
    private JobTrackerApplication application;

    public ClientNotifier(JobTrackerApplication application, ClientNotifyHandler clientNotifyHandler) {
        this.application = application;
        this.clientNotifyHandler = clientNotifyHandler;
    }

    /**
     * 发送给客户端
     * @return 返回成功的个数
     */
    public <T extends TaskTrackerJobResult> int send(List<T> jobResults) {
        if (CollectionUtils.isEmpty(jobResults)) {
            return 0;
        }

        // 单个 就不用 分组了
        if (jobResults.size() == 1) {

            TaskTrackerJobResult result = jobResults.get(0);
            if (!send0(result.getJobWrapper().getJob().getSubmitNodeGroup(), Arrays.asList(result))) {
                // 如果没有完成就返回
                clientNotifyHandler.handleFailed(jobResults);
                return 0;
            }
        } else if (jobResults.size() > 1) {

            List<TaskTrackerJobResult> failedTaskTrackerJobResult = new ArrayList<TaskTrackerJobResult>();

            // 有多个要进行分组 (出现在 失败重发的时候)
            Map<String/*nodeGroup*/, List<TaskTrackerJobResult>> groupMap = new HashMap<String, List<TaskTrackerJobResult>>();

            for (T jobResult : jobResults) {
                List<TaskTrackerJobResult> results = groupMap.get(jobResult.getJobWrapper().getJob().getSubmitNodeGroup());
                if (results == null) {
                    results = new ArrayList<TaskTrackerJobResult>();
                    groupMap.put(jobResult.getJobWrapper().getJob().getSubmitNodeGroup(), results);
                }
                results.add(jobResult);
            }
            for (Map.Entry<String, List<TaskTrackerJobResult>> entry : groupMap.entrySet()) {

                if (!send0(entry.getKey(), entry.getValue())) {
                    failedTaskTrackerJobResult.addAll(entry.getValue());
                }
            }
            clientNotifyHandler.handleFailed(failedTaskTrackerJobResult);
            return jobResults.size() - failedTaskTrackerJobResult.size();
        }
        return jobResults.size();
    }

    /**
     * 发送给客户端
     * 返回是否发送成功还是失败
     */
    private boolean send0(String nodeGroup, final List<TaskTrackerJobResult> results) {
        // 得到 可用的客户端节点
        JobClientNode jobClientNode = application.getJobClientManager().getAvailableJobClient(nodeGroup);

        if (jobClientNode == null) {
            return false;
        }
        List<JobResult> jobResults = new ArrayList<JobResult>(results.size());
        for (TaskTrackerJobResult result : results) {
            JobResult jobResult = new JobResult();
            jobResult.setJob(result.getJobWrapper().getJob());
            jobResult.setSuccess(Action.EXECUTE_SUCCESS.equals(result.getAction()));
            jobResult.setMsg(result.getMsg());
            jobResult.setTime(result.getTime());
            jobResults.add(jobResult);
        }

        JobFinishedRequest requestBody = application.getCommandBodyWrapper().wrapper(new JobFinishedRequest());
        requestBody.setJobResults(jobResults);
        RemotingCommand commandRequest = RemotingCommand.createRequestCommand(JobProtos.RequestCode.JOB_FINISHED.code(), requestBody);

        final Holder<Boolean> result = new Holder<Boolean>();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            getRemotingServer().invokeAsync(jobClientNode.getChannel().getChannel(), commandRequest, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    try {
                        RemotingCommand commandResponse = responseFuture.getResponseCommand();

                        if (commandResponse != null && commandResponse.getCode() == JobProtos.ResponseCode.JOB_NOTIFY_SUCCESS.code()) {
                            clientNotifyHandler.handleSuccess(results);
                            result.set(true);
                        } else {
                            result.set(false);
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });

            try {
                latch.await(Constants.LATCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RequestTimeoutException(e);
            }

        } catch (RemotingSendException e) {
            LOGGER.error("Notify client failed!", e);
        }
        return result.get();
    }

    private RemotingServerDelegate getRemotingServer() {
        return application.getRemotingServer();
    }

}
