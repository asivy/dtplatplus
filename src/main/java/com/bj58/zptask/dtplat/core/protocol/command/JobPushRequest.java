package com.bj58.zptask.dtplat.core.protocol.command;

import com.bj58.zptask.dtplat.annotation.NotNull;
import com.bj58.zptask.dtplat.core.domain.JobWrapper;

/**
 * @author Robert HG (254963746@qq.com) on 8/14/14.
 */
public class JobPushRequest extends AbstractCommandBody {

    @NotNull
    private JobWrapper jobWrapper;

    public JobWrapper getJobWrapper() {
        return jobWrapper;
    }

    public void setJobWrapper(JobWrapper jobWrapper) {
        this.jobWrapper = jobWrapper;
    }
}
