package com.bj58.zptask.dtplat.tasktracker.runner;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.domain.Action;
import com.bj58.zptask.dtplat.core.domain.JobWrapper;
import com.bj58.zptask.dtplat.core.support.SystemClock;
import com.bj58.zptask.dtplat.tasktracker.Result;
import com.bj58.zptask.dtplat.tasktracker.domain.Response;
import com.bj58.zptask.dtplat.tasktracker.domain.TaskTrackerApplication;

/**
 * @author Robert HG (254963746@qq.com) on 8/16/14.
 *         Job Runner 的代理类,  要做一些错误处理之类的
 */
public class JobRunnerDelegate implements Runnable {

    private static final Logger logger = Logger.getLogger(JobRunnerDelegate.class);

    private JobWrapper jobWrapper;
    private RunnerCallback callback;
    private TaskTrackerApplication application;

    public JobRunnerDelegate(TaskTrackerApplication application, JobWrapper jobWrapper, RunnerCallback callback) {
        this.jobWrapper = jobWrapper;
        this.callback = callback;
        this.application = application;
    }

    @Override
    public void run() {
        try {

            while (jobWrapper != null) {
                long startTime = SystemClock.now();
                // 设置当前context中的jobId
                Response response = new Response();
                response.setJobWrapper(jobWrapper);
                try {
                    application.getRunnerPool().getRunningJobManager().in(jobWrapper.getJobId());
                    Result result = application.getRunnerPool().getRunnerFactory().newRunner().run(jobWrapper.getJob());
                    if (result == null) {
                        response.setAction(Action.EXECUTE_SUCCESS);
                    } else {
                        Action action = result.getAction();
                        if (result.getAction() == null) {
                            action = Action.EXECUTE_SUCCESS;
                        }
                        response.setAction(action);
                        response.setMsg(result.getMsg());
                    }
                } catch (Throwable t) {
                    StringWriter sw = new StringWriter();
                    t.printStackTrace(new PrintWriter(sw));
                    response.setAction(Action.EXECUTE_EXCEPTION);
                    response.setMsg(sw.toString());
                } finally {
                    application.getRunnerPool().getRunningJobManager().out(jobWrapper.getJobId());
                }
                jobWrapper = callback.runComplete(response);
            }
        } finally {
        }
    }

}
