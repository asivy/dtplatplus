package com.bj58.zptask.dtplat.jobtracker.support;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.bj58.zhaopin.feature.entity.TaskDefineBean;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.entity.TaskExecutingBean;
import com.bj58.zhaopin.feature.entity.TaskFeedbackBean;
import com.bj58.zhaopin.feature.entity.TaskLogBean;
import com.bj58.zptask.dtplat.core.domain.Job;
import com.bj58.zptask.dtplat.core.domain.JobFeedbackPo;
import com.bj58.zptask.dtplat.core.domain.JobPo;
import com.bj58.zptask.dtplat.core.domain.JobWrapper;
import com.bj58.zptask.dtplat.core.domain.TaskTrackerJobResult;
import com.bj58.zptask.dtplat.core.support.SystemClock;
import com.bj58.zptask.dtplat.util.SnowFlakeUuid;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 * 
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月14日 上午11:45:19
 * @see 
 * @since
 */
public class JobDomainConverter {

    private static final Logger logger = Logger.getLogger(JobDomainConverter.class);

    public static JobPo convert(Job job) {
        JobPo jobPo = new JobPo();
        jobPo.setPriority(job.getPriority());
        jobPo.setTaskId(job.getTaskId());
        jobPo.setGmtModified(SystemClock.now());
        jobPo.setSubmitNodeGroup(job.getSubmitNodeGroup());
        jobPo.setTaskTrackerNodeGroup(job.getTaskTrackerNodeGroup());
        jobPo.setExtParams(job.getExtParams());
        jobPo.setNeedFeedback(job.isNeedFeedback());
        jobPo.setCronExpression(job.getCronExpression());
        if (!jobPo.isSchedule()) {
            if (job.getTriggerTime() == null) {
                jobPo.setTriggerTime(SystemClock.now());
            } else {
                jobPo.setTriggerTime(job.getTriggerTime());
            }
        }
        logger.info("conver job to jobpo");
        return jobPo;
    }

    /**
     * JobPo 转 Job
     */
    public static JobWrapper convert(JobPo jobPo) {
        Job job = new Job();
        job.setPriority(jobPo.getPriority());
        job.setExtParams(jobPo.getExtParams());
        job.setSubmitNodeGroup(jobPo.getSubmitNodeGroup());
        job.setTaskId(jobPo.getTaskId());
        job.setTaskTrackerNodeGroup(jobPo.getTaskTrackerNodeGroup());
        job.setNeedFeedback(jobPo.isNeedFeedback());
        job.setCronExpression(jobPo.getCronExpression());
        job.setTriggerTime(jobPo.getTriggerTime());
        job.setRetryTimes(jobPo.getRetryTimes() == null ? 0 : jobPo.getRetryTimes());
        logger.info("convert jobpo to jobwrapper");
        return new JobWrapper(jobPo.getJobId(), job);
    }

    //    public static JobLogPo convertJobLog(JobWrapper jobWrapper) {
    //        JobLogPo jobLogPo = new JobLogPo();
    //        jobLogPo.setGmtCreated(SystemClock.now());
    //        Job job = jobWrapper.getJob();
    //        jobLogPo.setPriority(job.getPriority());
    //        jobLogPo.setExtParams(job.getExtParams());
    //        jobLogPo.setSubmitNodeGroup(job.getSubmitNodeGroup());
    //        jobLogPo.setTaskId(job.getTaskId());
    //        jobLogPo.setTaskTrackerNodeGroup(job.getTaskTrackerNodeGroup());
    //        jobLogPo.setNeedFeedback(job.isNeedFeedback());
    //        jobLogPo.setRetryTimes(job.getRetryTimes());
    //        jobLogPo.setJobId(jobWrapper.getJobId());
    //        jobLogPo.setCronExpression(job.getCronExpression());
    //        jobLogPo.setTriggerTime(job.getTriggerTime());
    //        return jobLogPo;
    //    }

    public static TaskLogBean convertJobLogToTaskLog(JobWrapper jobWrapper) {
        TaskLogBean tasklog = new TaskLogBean();
        tasklog.setCreateDate(new Date());
        Job job = jobWrapper.getJob();
        tasklog.setTaskId(job.getTaskId());
        tasklog.setTaskGroup(job.getTaskTrackerNodeGroup());
        tasklog.setRetryTimes(job.getRetryTimes());
        tasklog.setJobId(Long.parseLong(jobWrapper.getJobId()));
        tasklog.setCronExpression(job.getCronExpression());
        tasklog.setTriggerTime(new Date(job.getTriggerTime()));
        logger.info("convert jobwrapper to tasklog");
        return tasklog;
    }

    //    public static JobLogPo convertJobLog(JobPo jobPo) {
    //        JobLogPo jobLogPo = new JobLogPo();
    //        jobLogPo.setGmtCreated(SystemClock.now());
    //        jobLogPo.setPriority(jobPo.getPriority());
    //        jobLogPo.setExtParams(jobPo.getExtParams());
    //        jobLogPo.setSubmitNodeGroup(jobPo.getSubmitNodeGroup());
    //        jobLogPo.setTaskId(jobPo.getTaskId());
    //        jobLogPo.setTaskTrackerNodeGroup(jobPo.getTaskTrackerNodeGroup());
    //        jobLogPo.setNeedFeedback(jobPo.isNeedFeedback());
    //        jobLogPo.setJobId(jobPo.getJobId());
    //        jobLogPo.setCronExpression(jobPo.getCronExpression());
    //        jobLogPo.setTriggerTime(jobPo.getTriggerTime());
    //        jobLogPo.setTaskTrackerIdentity(jobPo.getTaskTrackerIdentity());
    //        jobLogPo.setRetryTimes(jobPo.getRetryTimes());
    //        return jobLogPo;
    //    }

    public static TaskLogBean convertJobToTaskLog(JobPo jobPo) {
        TaskLogBean logbean = new TaskLogBean();
        logbean.setCreateDate(new Date());
        logbean.setTaskId(jobPo.getTaskId());
        logbean.setTaskGroup(jobPo.getTaskTrackerNodeGroup());
        logbean.setJobId(Long.parseLong(jobPo.getJobId()));
        logbean.setCronExpression(jobPo.getCronExpression());
        logbean.setTriggerTime(new Date(jobPo.getTriggerTime()));
        logbean.setTaskIdentity(jobPo.getTaskTrackerIdentity());
        logbean.setRetryTimes(jobPo.getRetryTimes());
        logger.info("convert jobpo to tasklog");
        return logbean;
    }

    public static TaskLogBean convertExecutingToTaskLog(TaskExecutingBean taskExecuting) {
        TaskLogBean logbean = new TaskLogBean();
        logbean.setCreateDate(new Date());
        logbean.setTaskId(taskExecuting.getTaskId());
        logbean.setTaskGroup(taskExecuting.getTaskGroup());
        logbean.setJobId(taskExecuting.getJobId());
        logbean.setCronExpression(taskExecuting.getCronExpression());
        logbean.setTriggerTime(taskExecuting.getTriggerDate());
        logbean.setTaskIdentity(taskExecuting.getTaskIdentity());
        logbean.setRetryTimes(taskExecuting.getRetryTimes());
        logger.info("convert executing to tasklog");
        return logbean;
    }

    public static JobFeedbackPo convert(TaskTrackerJobResult result) {
        JobFeedbackPo jobFeedbackPo = new JobFeedbackPo();
        jobFeedbackPo.setTaskTrackerJobResult(result);
        jobFeedbackPo.setId(StringUtils.generateUUID());
        jobFeedbackPo.setGmtCreated(SystemClock.now());
        return jobFeedbackPo;
    }

    public static TaskFeedbackBean convertToFeedBack(TaskTrackerJobResult result) {
        TaskFeedbackBean feedback = new TaskFeedbackBean();
        feedback.setResult(JSON.toJSONString(result));
        feedback.setCreateDate(new Date());
        logger.info("convert result to feedback");
        return feedback;
    }

    public static TaskDefineBean convertjobToTaskDefine(Job job) {
        TaskDefineBean taskDefine = new TaskDefineBean();
        //jobid应该是附加进去的
        taskDefine.setJobId(SnowFlakeUuid.getInstance().nextId());
        taskDefine.setPriority(job.getPriority());
        taskDefine.setCreateDate(new Date());
        taskDefine.setModifyDate(new Date());
        taskDefine.setSubmitGroup(job.getSubmitNodeGroup());
        taskDefine.setTaskGroup(job.getTaskTrackerNodeGroup());
        taskDefine.setExtParams(JSON.toJSONString(job.getExtParams()));
        taskDefine.setCronExpression(job.getCronExpression());
        taskDefine.setTriggerDate(new Date(job.getTriggerTime()));
        taskDefine.setTaskId(job.getTaskId());
        taskDefine.setRetryTimes(job.getRetryTimes() == null ? 0 : job.getRetryTimes());
        return taskDefine;
    }

    public static TaskDefineBean convertjobPotoTaskDefine(JobPo jobPo) {
        TaskDefineBean taskDefine = new TaskDefineBean();
        taskDefine.setPriority(jobPo.getPriority());

        taskDefine.setJobId(Long.parseLong(jobPo.getJobId()));
        taskDefine.setExtParams(JSON.toJSONString(jobPo.getExtParams()));
        taskDefine.setSubmitGroup(jobPo.getSubmitNodeGroup());
        taskDefine.setCreateDate(new Date(jobPo.getGmtCreated()));
        taskDefine.setModifyDate(new Date(jobPo.getGmtModified()));
        taskDefine.setTaskId(jobPo.getTaskId());
        taskDefine.setTaskGroup(jobPo.getTaskTrackerNodeGroup());
        taskDefine.setTaskIdentity(jobPo.getTaskTrackerIdentity());
        taskDefine.setCronExpression(jobPo.getCronExpression());
        taskDefine.setTriggerDate(new Date(jobPo.getTriggerTime()));
        taskDefine.setRetryTimes(jobPo.getRetryTimes() == null ? 0 : jobPo.getRetryTimes());
        return taskDefine;
    }

    public static TaskExecutableBean convertjobPotoTaskExecutable(JobPo jobPo) {
        TaskExecutableBean taskExecutable = new TaskExecutableBean();
        taskExecutable.setPriority(jobPo.getPriority());
        taskExecutable.setJobId(Long.parseLong(jobPo.getJobId()));
        taskExecutable.setExtParams(JSON.toJSONString(jobPo.getExtParams()));
        taskExecutable.setSubmitGroup(jobPo.getSubmitNodeGroup());
        taskExecutable.setCreateDate(new Date(jobPo.getGmtCreated()));
        taskExecutable.setModifyDate(new Date(jobPo.getGmtModified()));
        taskExecutable.setTaskId(jobPo.getTaskId());
        taskExecutable.setTaskGroup(jobPo.getTaskTrackerNodeGroup());
        taskExecutable.setTaskIdentity(jobPo.getTaskTrackerIdentity());
        taskExecutable.setCronExpression(jobPo.getCronExpression());
        taskExecutable.setTriggerDate(new Date(jobPo.getTriggerTime()));
        taskExecutable.setRunning(jobPo.isRunning());
        taskExecutable.setRetryTimes(jobPo.getRetryTimes() == null ? 0 : jobPo.getRetryTimes());
        return taskExecutable;
    }

    /**
     * 把taskexecutablebean  转成 jobpo
     * @param taskexecutable
     * @return
     */
    public static JobPo convertTaskExecutableToJobpo(TaskExecutableBean taskexecutable) {
        if (taskexecutable == null) {
            return null;
        }
        JobPo jobPo = new JobPo();
        jobPo.setPriority(taskexecutable.getPriority());
        jobPo.setJobId(taskexecutable.getJobId() + "");
        jobPo.setExtParams((Map<String, String>) JSON.parse((taskexecutable.getExtParams())));
        jobPo.setSubmitNodeGroup(taskexecutable.getSubmitGroup());
        jobPo.setGmtCreated(taskexecutable.getCreateDate().getTime());
        jobPo.setGmtModified(taskexecutable.getModifyDate().getTime());
        jobPo.setTaskId(taskexecutable.getTaskId());
        jobPo.setTaskTrackerNodeGroup(taskexecutable.getTaskGroup());
        jobPo.setIsRunning(taskexecutable.isRunning());
        jobPo.setCronExpression(taskexecutable.getCronExpression());
        jobPo.setTaskTrackerIdentity(taskexecutable.getTaskIdentity());
        jobPo.setTriggerTime(taskexecutable.getTriggerDate().getTime());
        jobPo.setRetryTimes(taskexecutable.getRetryTimes());
        jobPo.setIsRunning(taskexecutable.isRunning());
        logger.info("convert executable to jobpo");
        return jobPo;
    }

    public static TaskExecutingBean convertjobPotoTaskExecuting(JobPo jobPo) {
        TaskExecutingBean taskExecuting = new TaskExecutingBean();
        taskExecuting.setPriority(jobPo.getPriority());
        taskExecuting.setJobId(Long.parseLong(jobPo.getJobId()));
        taskExecuting.setExtParams(JSON.toJSONString(jobPo.getExtParams()));
        taskExecuting.setSubmitGroup(jobPo.getSubmitNodeGroup());
        taskExecuting.setCreateDate(new Date(jobPo.getGmtCreated()));
        taskExecuting.setModifyDate(new Date(jobPo.getGmtModified()));
        taskExecuting.setTaskId(jobPo.getTaskId());
        taskExecuting.setTaskGroup(jobPo.getTaskTrackerNodeGroup());
        taskExecuting.setTaskIdentity(jobPo.getTaskTrackerIdentity());
        taskExecuting.setCronExpression(jobPo.getCronExpression());
        taskExecuting.setTriggerDate(new Date(jobPo.getTriggerTime()));
        taskExecuting.setRetryTimes(jobPo.getRetryTimes() == null ? 0 : jobPo.getRetryTimes());
        taskExecuting.setRunning(jobPo.isRunning());
        logger.info("convert jobbo to executing");
        return taskExecuting;
    }

    public static JobPo convertTaskExecutingToJobpo(TaskExecutingBean taskExecuting) {
        JobPo jobPo = new JobPo();
        if (taskExecuting != null) {
            return null;
        }
        jobPo.setPriority(taskExecuting.getPriority());
        jobPo.setJobId(taskExecuting.getJobId() + "");
        jobPo.setExtParams((Map<String, String>) JSON.parse((taskExecuting.getExtParams())));
        jobPo.setSubmitNodeGroup(taskExecuting.getSubmitGroup());
        jobPo.setGmtCreated(taskExecuting.getCreateDate().getTime());
        jobPo.setGmtModified(taskExecuting.getModifyDate().getTime());
        jobPo.setTaskId(taskExecuting.getTaskId());
        jobPo.setTaskTrackerNodeGroup(taskExecuting.getTaskGroup());
        jobPo.setTaskTrackerIdentity(taskExecuting.getTaskIdentity());
        jobPo.setCronExpression(taskExecuting.getCronExpression());
        jobPo.setTriggerTime(taskExecuting.getTriggerDate().getTime());
        jobPo.setRetryTimes(taskExecuting.getRetryTimes());
        jobPo.setIsRunning(taskExecuting.isRunning());
        logger.info("convert executing to jobpo");
        return jobPo;
    }

    public static TaskExecutableBean convertExectuingToExecutable(TaskExecutingBean taskExecuting) {
        TaskExecutableBean taskExecutable = new TaskExecutableBean();
        taskExecutable.setPriority(taskExecuting.getPriority());
        taskExecutable.setExtParams(taskExecuting.getExtParams());
        taskExecutable.setJobId(taskExecuting.getJobId());
        taskExecutable.setSubmitGroup(taskExecuting.getSubmitGroup());
        taskExecutable.setCreateDate(taskExecuting.getCreateDate());
        taskExecutable.setModifyDate(taskExecuting.getModifyDate());
        taskExecutable.setTaskId(taskExecuting.getTaskId());
        taskExecutable.setTaskIdentity(taskExecuting.getTaskIdentity());
        taskExecutable.setTaskGroup(taskExecuting.getTaskGroup());
        taskExecutable.setCronExpression(taskExecuting.getCronExpression());
        taskExecutable.setTriggerDate(taskExecuting.getTriggerDate());
        taskExecutable.setRetryTimes(taskExecuting.getRetryTimes());
        logger.info("convert executing to executable");
        return taskExecutable;
    }

    public static JobPo convertTaskdefineToJobPo(TaskDefineBean taskDefineBean) {
        JobPo jobpo = new JobPo();
        jobpo.setPriority(taskDefineBean.getPriority());
        jobpo.setTaskId(taskDefineBean.getTaskId());
        jobpo.setGmtModified(taskDefineBean.getModifyDate().getTime());
        jobpo.setSubmitNodeGroup(taskDefineBean.getSubmitGroup());
        jobpo.setTaskTrackerNodeGroup(taskDefineBean.getTaskGroup());
        Map<String, String> map = JSON.parseObject(taskDefineBean.getExtParams(), new TypeReference<Map<String, String>>() {
        });
        jobpo.setExtParams(map);
        jobpo.setCronExpression(taskDefineBean.getCronExpression());
        jobpo.setGmtCreated(taskDefineBean.getCreateDate().getTime());
        jobpo.setJobId(String.valueOf(taskDefineBean.getJobId()));
        jobpo.setRetryTimes(taskDefineBean.getRetryTimes());
        jobpo.setTaskTrackerIdentity(taskDefineBean.getTaskIdentity());
        jobpo.setTriggerTime(taskDefineBean.getTriggerDate().getTime());
        logger.info("convert define to jobpo");
        return jobpo;

    }
}
