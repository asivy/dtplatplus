package com.bj58.zptask.dtplat.jobtracker.processor;

import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.core.protocol.JobProtos;
import com.bj58.zptask.dtplat.core.protocol.command.JobPullRequest;
import com.bj58.zptask.dtplat.exception.RemotingCommandException;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.jobtracker.support.JobPusher;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

/**
 * tasktracker拉取任务
 * TaskTracker的 Job pull 请求
 */
public class JobPullProcessor extends AbstractProcessor {

    private JobPusher jobPusher;

    private static final Logger LOGGER = LoggerFactory.getLogger(JobPullProcessor.class);

    public JobPullProcessor(RemotingServerDelegate remotingServer, JobTrackerApplication application) {
        super(remotingServer, application);

        jobPusher = new JobPusher(application);
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, final RemotingCommand request) throws RemotingCommandException {
        System.out.println("get task pull request");
        JobPullRequest requestBody = request.getBody();
        
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("taskTrackerNodeGroup:{}, taskTrackerIdentity:{} , availableThreads:{}", requestBody.getNodeGroup(), requestBody.getIdentity(), requestBody.getAvailableThreads());
        }
        jobPusher.push(remotingServer, requestBody);

        return RemotingCommand.createResponseCommand(JobProtos.ResponseCode.JOB_PULL_SUCCESS.code(), "");
    }
}
