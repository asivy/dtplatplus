package com.bj58.zptask.dtplat.jobtracker.handler;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.entity.TaskExecutingBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.rpc.netty5.MessageType;
import com.bj58.zptask.dtplat.rpc.netty5.NettyMessage;
import com.bj58.zptask.dtplat.util.GenericsUtils;
import com.bj58.zptask.dtplat.util.SerializeUtil;

/**
 * 拉取待执行的任务
 * 
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月15日 下午1:21:49
 * @see 
 * @since
 */
public class TaskPullHandler extends ChannelHandlerAdapter {

    private static final Logger logger = Logger.getLogger(TaskPullHandler.class);
    private static final ConcurrentHashMap<String/*taskgroup*/, ReentrantLock> lockMap = GenericsUtils.newConcurrentHashMap();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        NettyMessage msg = (NettyMessage) obj;
        if (msg.getType() == MessageType.TASK_PULL.value()) {
            logger.info(String.format("deal %s ", msg.getUuid()));
            ctx.writeAndFlush(processMsg(msg));
        } else {
            ctx.fireChannelRead(obj);
        }
    }

    /**
     * 1 获取待执任务；
     * 2 添加待执行任务
     * 3 删除待执行任务；
     * 4 如果执行失败 需要重置待执行任务；
     * 5 ？ 怎样保证当有多个task时  不会有多个task执行此功能；  这个可以通过锁或countdownlatch做到
     * 6 ? 分布式情况下怎样保证事务； 这个没有做到  
     * @param msg
     * @return
     */
    private NettyMessage processMsg(NettyMessage msg) {
        msg.setTimestamp(System.currentTimeMillis());
        msg.setType(MessageType.TASK_PULL_RES.value());
        String taskGroup = msg.getNodeGroup();
        ReentrantLock lock = lockMap.get(taskGroup);
        if (lock == null) {
            lock = new ReentrantLock();
            lockMap.put(taskGroup, lock);
        }
        long jobid = 0l;
        try {
            lock.tryLock(2000, TimeUnit.MILLISECONDS);

            String taskIdentity = msg.getIdentity();
            TaskExecutableBean task = InjectorHolder.getInstance(DTaskProvider.class).takeExecutaleTask(taskGroup, taskIdentity);
            if (task == null) {
                msg.setJsonobj("");
                return msg;
            }
            jobid = task.getJobId();

            TaskExecutingBean taskExecuting = executableToExecutng(task);

            InjectorHolder.getInstance(DTaskProvider.class).addExecutingTask(taskExecuting);
            InjectorHolder.getInstance(DTaskProvider.class).deleteExecutableTask(jobid, taskGroup);
            msg.setJsonobj(SerializeUtil.jsonSerialize(taskExecuting));
            return msg;
        } catch (Exception e) {
            logger.error(e);
            if (jobid > 0) {
                InjectorHolder.getInstance(DTaskProvider.class).resetExecutableTask(jobid, taskGroup);
            }
        } finally {
            lock.unlock();
        }
        return msg;
    }

    /**
     * 待执行  转化为    正在执行
     * @param task
     * @return
     */
    private TaskExecutingBean executableToExecutng(TaskExecutableBean task) {
        TaskExecutingBean executing = new TaskExecutingBean();
        executing.setCreateDate(new Date());
        executing.setCronExpression(task.getCronExpression());
        executing.setExtParams(task.getExtParams());
        executing.setJobId(task.getJobId());
        executing.setModifyDate(new Date());
        executing.setPriority(task.getPriority());
        executing.setRunning(true);
        executing.setSubmitGroup(task.getSubmitGroup());
        executing.setTaskGroup(task.getTaskGroup());
        executing.setTaskId(task.getTaskId());
        executing.setTaskIdentity(task.getTaskIdentity());
        executing.setTriggerDate(task.getTriggerDate());
        return executing;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
