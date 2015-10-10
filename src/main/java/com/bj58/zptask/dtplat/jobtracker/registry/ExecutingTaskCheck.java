package com.bj58.zptask.dtplat.jobtracker.registry;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.entity.TaskExecutingBean;
import com.bj58.zhaopin.feature.entity.TaskLogBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.exception.DuplicateJobException;
import com.bj58.zptask.dtplat.jobtracker.Damon;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelManager;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelWrapper;
import com.bj58.zptask.dtplat.jobtracker.support.JobDomainConverter;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.util.JSONUtils;
import com.google.inject.Singleton;

/**
 * 检查正在执行的任务
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午8:09:17
 * @see 
 * @since
 */
@Singleton
public class ExecutingTaskCheck implements Damon {

    private static final Logger logger = Logger.getLogger(ExecutingTaskCheck.class);

    @Override
    public void start() {
        logger.info(String.format("%sExecuting Task Check Start%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    /**
     * 修复一个节节上的僵尸任务  
     * 当一个Task节点被删除时调用  
     * 1 获取此节点正在执行的任务
     * 2 增加一个待执行任务
     * 3 停掉此正在执行的任务
     * 因为Task节点已经停了   所以该任务已经终止了
     * @param node
     */
    public void fixDeadTaskByNode(Node node) {
        try {
            ChannelManager channelManager = InjectorHolder.getInstance(ChannelManager.class);

            ChannelWrapper channelWrapper = channelManager.getChannel(node.getGroup(), node.getNodeType(), node.getIdentity());
            if (channelWrapper == null || channelWrapper.getChannel() == null || channelWrapper.isClosed()) {

                List<TaskExecutingBean> executingTasks = InjectorHolder.getInstance(DTaskProvider.class).loadTaskExecutingTasksBytaskTrackerIdentity(node.getIdentity());
                if (CollectionUtils.isNotEmpty(executingTasks)) {
                    for (TaskExecutingBean taskExectuing : executingTasks) {
                        fixDeadTask(taskExectuing);
                    }
                }
            }
        } catch (Exception t) {
            logger.error(t);
        }
    }

    /**
     * 修复僵尸任务
     * 
     * @param taskExecuting
     */
    private void fixDeadTask(TaskExecutingBean taskExecuting) {
        try {
            taskExecuting.setRunning(false);
            try {
                TaskExecutableBean taskExecutable = JobDomainConverter.convertExectuingToExecutable(taskExecuting);
                InjectorHolder.getInstance(DTaskProvider.class).addExecutableTask(taskExecutable);
            } catch (DuplicateJobException e) {
                // ignore
            }
            InjectorHolder.getInstance(DTaskProvider.class).deleteExecutingTaskByJobID(taskExecuting.getJobId());

            TaskLogBean entity = JobDomainConverter.convertExecutingToTaskLog(taskExecuting);
            entity.setSuccess(true);
            entity.setLogType("FIXED_DEAD");
            entity.setCreateDate(new Date());
            entity.setLevel("WARN");
            //打下日志
            InjectorHolder.getInstance(DTaskProvider.class).addTaskLog(entity);
        } catch (Throwable t) {
            logger.error(t);
        }
        logger.info(String.format("fix dead task %s ", JSONUtils.toJSONString(taskExecuting)));
    }

    @Override
    public void stop() throws Exception {
        logger.info(String.format("%sExecuting Task Check Stop%s", Constants.LOGTIP, Constants.LOGTIP));
    }
}
