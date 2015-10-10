//package com.bj58.zptask.dtplat.jobtracker.processor;
//
//import com.bj58.zptask.dtplat.core.logger.Logger;
//import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
//import com.bj58.zptask.dtplat.core.protocol.JobProtos;
//import com.bj58.zptask.dtplat.core.protocol.command.JobSubmitRequest;
//import com.bj58.zptask.dtplat.core.protocol.command.JobSubmitResponse;
//import com.bj58.zptask.dtplat.exception.JobReceiveException;
//import com.bj58.zptask.dtplat.exception.RemotingCommandException;
//import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
//import com.bj58.zptask.dtplat.jobtracker.support.JobReceiver;
//import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
//import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;
//
//import io.netty.channel.ChannelHandlerContext;
//
///**
// *   处理客户端提交的任务
// *   客户端提交任务的处理器
// *   这个是JOBCLIENT提交的   暂时不需要了
// */
//public class JobSubmitProcessor extends AbstractProcessor {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(JobSubmitProcessor.class);
//
//    private JobReceiver jobReceiver;
//
//    public JobSubmitProcessor(RemotingServerDelegate remotingServer, JobTrackerApplication application) {
//        super(remotingServer, application);
//        this.jobReceiver = new JobReceiver(application);
//    }
//
//    @Override
//    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
//
//        JobSubmitRequest jobSubmitRequest = request.getBody();
//
//        JobSubmitResponse jobSubmitResponse = application.getCommandBodyWrapper().wrapper(new JobSubmitResponse());
//        RemotingCommand response = null;
//        try {
//            jobReceiver.receive(jobSubmitRequest);
//
//            response = RemotingCommand.createResponseCommand(JobProtos.ResponseCode.JOB_RECEIVE_SUCCESS.code(), "job submit success!", jobSubmitResponse);
//
//        } catch (JobReceiveException e) {
//            LOGGER.error("receive job failed , jobs = " + jobSubmitRequest.getJobs(), e);
//            jobSubmitResponse.setSuccess(false);
//            jobSubmitResponse.setMsg(e.getMessage());
//            jobSubmitResponse.setFailedJobs(e.getJobs());
//            response = RemotingCommand.createResponseCommand(JobProtos.ResponseCode.JOB_RECEIVE_FAILED.code(), e.getMessage(), jobSubmitResponse);
//        }
//
//        return response;
//    }
//}
