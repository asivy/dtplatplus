package com.bj58.zptask.dtplat.exception;

import java.util.ArrayList;
import java.util.List;

import com.bj58.zptask.dtplat.core.domain.Job;

/**
 * @author Robert HG (254963746@qq.com) on 8/1/14. 客户端提交的任务 接受 异常
 */
public class JobReceiveException extends Exception {

    private static final long serialVersionUID = 5162593889993338368L;
    /**
     * 出错的job列表
     */
    private List<Job> jobs;

    public List<Job> getJobs() {
        return jobs;
    }

    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }

    public void addJob(Job job) {
        if (jobs == null) {
            jobs = new ArrayList<Job>();
        }

        jobs.add(job);
    }

    public JobReceiveException() {
    }

    public JobReceiveException(String message) {
        super(message);
    }

    public JobReceiveException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobReceiveException(Throwable cause) {
        super(cause);
    }

    public JobReceiveException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
