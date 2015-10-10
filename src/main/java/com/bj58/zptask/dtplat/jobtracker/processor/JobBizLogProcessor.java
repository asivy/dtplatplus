package com.bj58.zptask.dtplat.jobtracker.processor;

import io.netty.channel.ChannelHandlerContext;

import java.util.Date;
import java.util.List;

import com.bj58.zhaopin.feature.entity.TaskLogBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.domain.BizLog;
import com.bj58.zptask.dtplat.core.protocol.JobProtos;
import com.bj58.zptask.dtplat.core.protocol.command.BizLogSendRequest;
import com.bj58.zptask.dtplat.exception.RemotingCommandException;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;
import com.bj58.zptask.dtplat.util.CollectionUtils;

/**
 * @author Robert HG (254963746@qq.com) on 3/30/15.
 */
public class JobBizLogProcessor extends AbstractProcessor {

    public JobBizLogProcessor(RemotingServerDelegate remotingServer, JobTrackerApplication application) {
        super(remotingServer, application);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {

        BizLogSendRequest requestBody = request.getBody();

        List<BizLog> bizLogs = requestBody.getBizLogs();
        if (CollectionUtils.isNotEmpty(bizLogs)) {
            for (BizLog bizLog : bizLogs) {
                //                JobLogPo jobLogPo = new JobLogPo();
                //                jobLogPo.setGmtCreated(SystemClock.now());
                //                jobLogPo.setLogTime(bizLog.getLogTime());
                //                jobLogPo.setTaskTrackerNodeGroup(bizLog.getTaskTrackerNodeGroup());
                //                jobLogPo.setTaskTrackerIdentity(bizLog.getTaskTrackerIdentity());
                //                jobLogPo.setJobId(bizLog.getJobId());
                //                jobLogPo.setTaskId(bizLog.getTaskId());
                //                jobLogPo.setMsg(bizLog.getMsg());
                //                jobLogPo.setSuccess(true);
                //                jobLogPo.setLevel(bizLog.getLevel());
                //                jobLogPo.setLogType(LogType.BIZ);
                //                application.getJobLogger().log(jobLogPo);
                TaskLogBean tasklog = new TaskLogBean();
                tasklog.setCreateDate(new Date());
                tasklog.setCreateDate(new Date(bizLog.getLogTime()));
                tasklog.setTaskGroup(bizLog.getTaskTrackerNodeGroup());
                tasklog.setTaskIdentity(bizLog.getTaskTrackerIdentity());
                tasklog.setJobId(Long.parseLong(bizLog.getJobId()));
                tasklog.setTaskId(bizLog.getTaskId());
                tasklog.setMsg(bizLog.getMsg());
                tasklog.setSuccess(true);
                tasklog.setLevel("bizLog.getLevel()");
                tasklog.setLogType("BIZ");
                InjectorHolder.getInstance(DTaskProvider.class).addTaskLog(tasklog);

                //            InjectorHolder.getInstance(FeatureProvider.class).getIJobLogProService().insert(jobLogPo);

            }
        }

        return RemotingCommand.createResponseCommand(JobProtos.ResponseCode.BIZ_LOG_SEND_SUCCESS.code(), "");
    }
}
