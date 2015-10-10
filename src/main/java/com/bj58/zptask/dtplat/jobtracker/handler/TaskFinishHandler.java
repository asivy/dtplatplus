package com.bj58.zptask.dtplat.jobtracker.handler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Date;

import org.apache.log4j.Logger;

import com.bj58.zhaopin.feature.entity.TaskDefineBean;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.util.StringUtil;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.util.CronExpressionUtils;

/**
 * 处理完一个任务的后续处理
 *
 * 1 任务处理完成后  才从define生成到executable中    所以不会存在一个任务被多次执行的情况
 * 2 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月15日 下午7:31:55
 * @see 
 * @since
 */
public class TaskFinishHandler extends ChannelHandlerAdapter {

    private static final Logger logger = Logger.getLogger(TaskFinishHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        NettyMessage msg = (NettyMessage) obj;
        if (msg.getType() == MessageType.TASK_FINISH.value()) {
            ctx.writeAndFlush(processMsg(msg));
        } else {
            ctx.fireChannelRead(obj);
        }
    }

    /**
     * 1 删除正在执行的任务
     * 2 从 define 转化成executable 计算下次执行时间  并存入数据库中
     * @param msg
     * @return
     */
    private NettyMessage processMsg(NettyMessage msg) {
        long jobId = msg.getJobId();
        if (jobId < 1) {
            return msg;
        }
        msg.setTimestamp(System.currentTimeMillis());
        msg.setType(MessageType.TASK_FINISH_RES.value());
        try {
            InjectorHolder.getInstance(DTaskProvider.class).deleteExecutingTaskByJobID(jobId);
            TaskDefineBean define = InjectorHolder.getInstance(DTaskProvider.class).loadTaskDefineByJobID(jobId);
            if (define == null) {
                logger.info(String.format("define is null ,jobid=%s  ", jobId));
                return msg;
            }
            if (isScheduler(define)) {
                Date nextTrigger = CronExpressionUtils.getNextTriggerTime(define.getCronExpression());
                if (nextTrigger == null) {
                    logger.info(String.format("cron is invalid ,jobid=%s  ", jobId));
                } else {
                    TaskExecutableBean executable = defineToExecutable(define);
                    executable.setTriggerDate(nextTrigger);
                    InjectorHolder.getInstance(DTaskProvider.class).addExecutableTask(executable);
                }
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return msg;
    }

    private boolean isScheduler(TaskDefineBean define) {
        return !StringUtil.isNullOrEmpty(define.getCronExpression());
    }

    private TaskExecutableBean defineToExecutable(TaskDefineBean task) {
        TaskExecutableBean executable = new TaskExecutableBean();
        executable.setCreateDate(new Date());
        executable.setCronExpression(task.getCronExpression());
        executable.setExtParams(task.getExtParams());
        executable.setJobId(task.getJobId());
        executable.setModifyDate(new Date());
        executable.setPriority(task.getPriority());
        executable.setRunning(false);
        executable.setSubmitGroup(task.getSubmitGroup());
        executable.setTaskGroup(task.getTaskGroup());
        executable.setTaskId(task.getTaskId());
        executable.setTaskIdentity(task.getTaskIdentity());
        executable.setTriggerDate(task.getTriggerDate());
        return executable;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
