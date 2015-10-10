package com.bj58.zptask.dtplat.jobtracker.support.checker;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.JSONUtils;

/**
 * to fix the executable dead job
 *
 * @author Robert HG (254963746@qq.com) on 6/3/15.
 */
public class ExecutableDeadJobChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutableDeadJobChecker.class);

    // 1 分钟还锁着的，说明是有问题的  改为30分钟
    private static final long MAX_TIME_OUT = 30 * 1000;

    private final ScheduledExecutorService FIXED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

    private JobTrackerApplication application;

    public ExecutableDeadJobChecker(JobTrackerApplication application) {
        this.application = application;
    }

    private AtomicBoolean start = new AtomicBoolean(false);
    private ScheduledFuture<?> scheduledFuture;

    public void start() {
        try {
            if (start.compareAndSet(false, true)) {
                scheduledFuture = FIXED_EXECUTOR_SERVICE.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            fix();
                        } catch (Throwable t) {
                            LOGGER.error(t.getMessage(), t);
                        }
                    }
                }, 30, 60, TimeUnit.SECONDS);// 3分钟执行一次
            }
            LOGGER.info("Executable dead job checker started!");
        } catch (Throwable t) {
            LOGGER.info("Executable dead job checker start failed!");
        }
    }

    /**
     * 修复僵尸任务
     * 1 查询一分钟之前启动的任务；
     * 2 将任务的running task modify参数重置为
     * 
     * 这样做不好  因为确实有任务会执行很长时间
     * 
     * fix the job that running is true and gmtModified too old
     */
    private void fix() {
        Set<String> nodeGroups = application.getTaskTrackerManager().getNodeGroups();
        if (CollectionUtils.isEmpty(nodeGroups)) {
            return;
        }

        for (String nodeGroup : nodeGroups) {
            //            List<JobPo> deadJobPo = application.getExecutableJobQueue().getDeadJob(nodeGroup, SystemClock.now() - MAX_TIME_OUT);
            //            if (CollectionUtils.isNotEmpty(deadJobPo)) {
            //                for (JobPo jobPo : deadJobPo) {
            //                    application.getExecutableJobQueue().resume(jobPo);
            //                    LOGGER.info("Fix executable job : {} ", JSONUtils.toJSONString(jobPo));
            //                }
            //            }
            List<TaskExecutableBean> deadTasks = InjectorHolder.getInstance(DTaskProvider.class).loadDeadTask(nodeGroup, before(MAX_TIME_OUT));
            if (CollectionUtils.isNotEmpty(deadTasks)) {
                for (TaskExecutableBean taskExecutable : deadTasks) {
                    InjectorHolder.getInstance(DTaskProvider.class).resetExecutableTask(taskExecutable.getJobId(), taskExecutable.getTaskGroup());
                    LOGGER.info("Fix executable job : {} ", JSONUtils.toJSONString(taskExecutable));
                }
            }

        }
    }

    private Date before(long ms) {
        return new Date(System.currentTimeMillis() - ms);
    }

    public void stop() {
        try {
            if (start.compareAndSet(true, false)) {
                scheduledFuture.cancel(true);
                FIXED_EXECUTOR_SERVICE.shutdown();
            }
            LOGGER.info("Executable dead job checker stopped!");
        } catch (Throwable t) {
            LOGGER.error("Executable dead job checker stop failed!", t);
        }
    }
}
