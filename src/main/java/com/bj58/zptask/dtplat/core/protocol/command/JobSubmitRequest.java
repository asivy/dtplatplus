package com.bj58.zptask.dtplat.core.protocol.command;

import com.bj58.zptask.dtplat.annotation.NotNull;
import com.bj58.zptask.dtplat.core.domain.Job;

import java.util.List;

/**
 *  任务
 *  任务传递信息
 */
public class JobSubmitRequest extends AbstractCommandBody {

    @NotNull
    private List<Job> jobs;

    public List<Job> getJobs() {
        return jobs;
    }
    
    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }

}
