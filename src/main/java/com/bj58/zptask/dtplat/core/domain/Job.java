package com.bj58.zptask.dtplat.core.domain;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.quartz.CronExpression;

import com.bj58.zptask.dtplat.exception.JobSubmitException;
import com.bj58.zptask.dtplat.util.JSONUtils;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 * @author Robert HG (254963746@qq.com) on 8/13/14.
 */
public class Job {

    private String taskId;
    /**
     * 优先级 (数值越大 优先级越低)
     */
    private Integer priority = 100;
    // 提交的节点 （可以手动指定）
    private String submitNodeGroup;
    // 执行的节点
    private String taskTrackerNodeGroup;

    private Map<String, String> extParams;

    private boolean needFeedback = true; // 是否要反馈给客户端

    private int retryTimes = 0; // 重试次数
    private String cronExpression;//执行表达式 和 quartz 的一样 如果这个为空，表示立即执行的

    /**
     * 任务的最早出发时间 如果设置了 cronExpression， 那么这个字段没用
     */
    private Long triggerTime;

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getSubmitNodeGroup() {
        return submitNodeGroup;
    }

    public void setSubmitNodeGroup(String submitNodeGroup) {
        this.submitNodeGroup = submitNodeGroup;
    }

    public String getTaskTrackerNodeGroup() {
        return taskTrackerNodeGroup;
    }

    public void setTaskTrackerNodeGroup(String taskTrackerNodeGroup) {
        this.taskTrackerNodeGroup = taskTrackerNodeGroup;
    }

    public boolean isNeedFeedback() {
        return needFeedback;
    }

    public Integer getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setNeedFeedback(boolean needFeedback) {
        this.needFeedback = needFeedback;
    }

    public Map<String, String> getExtParams() {
        return extParams;
    }

    public void setExtParams(Map<String, String> extParams) {
        this.extParams = extParams;
    }

    public String getParam(String key) {
        if (extParams == null) {
            return null;
        }
        return extParams.get(key);
    }

    public void setParam(String key, String value) {
        if (extParams == null) {
            extParams = new HashMap<String, String>();
        }
        extParams.put(key, value);
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public boolean isSchedule() {
        return this.cronExpression != null && !"".equals(this.cronExpression.trim());
    }

    public void setTriggerTime(Date date) {
        if (date != null) {
            this.triggerTime = date.getTime();
        }
    }

    public Long getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(Long triggerTime) {
        this.triggerTime = triggerTime;
    }

    @Override
    public String toString() {
        return JSONUtils.toJSONString(this);
    }

    public void checkField() throws JobSubmitException {
        if (taskId == null) {
            throw new JobSubmitException("taskId can not be null! job is " + toString());
        }
        if (taskTrackerNodeGroup == null) {
            throw new JobSubmitException("taskTrackerNodeGroup can not be null! job is " + toString());
        }
        if (StringUtils.isNotEmpty(cronExpression) && !CronExpression.isValidExpression(cronExpression)) {
            throw new JobSubmitException("cronExpression invalid! job is " + toString());
        }
    }
}
