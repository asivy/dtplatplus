package com.bj58.zptask.dtplat.jobtracker.processor;

import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.core.protocol.JobProtos;
import com.bj58.zptask.dtplat.core.protocol.command.AbstractCommandBody;
import com.bj58.zptask.dtplat.exception.RemotingCommandException;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelWrapper;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.rpc.RemotingServerDelegate;
import com.bj58.zptask.dtplat.rpc.netty.NettyRequestProcessor;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingCommand;
import com.bj58.zptask.dtplat.rpc.protocol.RemotingProtos;

import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Map;

import static com.bj58.zptask.dtplat.core.protocol.JobProtos.RequestCode;

/**
 * 
 *   job tracker 总的处理器, 每一种命令对应不同的处理器
 *   类似netty中的handler
 *   此处简化成不同的HANDLER要好一些
 *   
 */
public class RemotingDispatcher extends AbstractProcessor {

    private final Map<RequestCode, NettyRequestProcessor> processors = new HashMap<RequestCode, NettyRequestProcessor>();

    public RemotingDispatcher(RemotingServerDelegate remotingServer, JobTrackerApplication application) {
        super(remotingServer, application);
        //        processors.put(RequestCode.SUBMIT_JOB, new JobSubmitProcessor(remotingServer, application));
        processors.put(RequestCode.JOB_FINISHED, new JobFinishedProcessor(remotingServer, application));
        processors.put(RequestCode.JOB_PULL, new JobPullProcessor(remotingServer, application));
        processors.put(RequestCode.BIZ_LOG_SEND, new JobBizLogProcessor(remotingServer, application));
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        // 心跳
        if (request.getCode() == JobProtos.RequestCode.HEART_BEAT.code()) {
            commonHandler(ctx, request);
            return RemotingCommand.createResponseCommand(JobProtos.ResponseCode.HEART_BEAT_SUCCESS.code(), "");
        }

        // 其他的请求code
        RequestCode code = RequestCode.valueOf(request.getCode());
        NettyRequestProcessor processor = processors.get(code);
        if (processor == null) {
            return RemotingCommand.createResponseCommand(RemotingProtos.ResponseCode.REQUEST_CODE_NOT_SUPPORTED.code(), "request code not supported!");
        }
        commonHandler(ctx, request);
        return processor.processRequest(ctx, request);
    }

    /**
     * 1. 将 channel 纳入管理中(不存在就加入)
     * 2. 更新 TaskTracker 节点信息(可用线程数)
     * 
     * @param ctx
     * @param request
     */
    private void commonHandler(ChannelHandlerContext ctx, RemotingCommand request) {
        AbstractCommandBody commandBody = request.getBody();
        String nodeGroup = commandBody.getNodeGroup();
        String identity = commandBody.getIdentity();
        NodeType nodeType = NodeType.valueOf(commandBody.getNodeType());

        // 1. 将 channel 纳入管理中(不存在就加入)
        application.getChannelManager().offerChannel(new ChannelWrapper(ctx.channel(), nodeType, nodeGroup, identity));
    }

}
