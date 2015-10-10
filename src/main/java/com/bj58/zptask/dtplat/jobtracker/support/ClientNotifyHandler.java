package com.bj58.zptask.dtplat.jobtracker.support;

import com.bj58.zptask.dtplat.core.domain.TaskTrackerJobResult;

import java.util.List;

/**
 * @author Robert HG (254963746@qq.com) on 3/3/15.
 */
public interface ClientNotifyHandler<T extends TaskTrackerJobResult> {

    /**
     * 通知成功的处理
     *
     * @param jobResults
     */
    public void handleSuccess(List<T> jobResults);

    /**
     * 通知失败的处理
     *
     * @param jobResults
     */
    public void handleFailed(List<T> jobResults);

}
