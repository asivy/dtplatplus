package com.bj58.zptask.dtplat.jobtracker.support.listener;

import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.listener.MasterChangeListener;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.jobtracker.support.checker.ExecutableDeadJobChecker;
import com.bj58.zptask.dtplat.jobtracker.support.checker.ExecutingDeadJobChecker;

/**
 * JOBTRACKER节点的Master状态发生变化后 的行为
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年8月26日 下午1:57:18
 * @see 
 * @since
 */
public class JobTrackerMasterChangeListener implements MasterChangeListener {

    private JobTrackerApplication application;
    private ExecutingDeadJobChecker executingDeadJobChecker;
    //    private FeedbackJobSendChecker feedbackJobSendChecker;
    private ExecutableDeadJobChecker executableDeadJobChecker;

    public JobTrackerMasterChangeListener(JobTrackerApplication application) {
        this.application = application;
        this.executingDeadJobChecker = new ExecutingDeadJobChecker(application);
        this.application.setExecutingDeadJobChecker(executingDeadJobChecker);
        //        this.feedbackJobSendChecker = new FeedbackJobSendChecker(application);
        this.executableDeadJobChecker = new ExecutableDeadJobChecker(application);
    }
    
    @Override
    public void change(Node master, boolean isMaster) {

        if (application.getConfig().getIdentity().equals(master.getIdentity())) {
            // 如果 master 节点是自己
            // 2. 启动通知客户端失败检查重发的定时器
            //            feedbackJobSendChecker.start();
            executingDeadJobChecker.start();
            executableDeadJobChecker.start();
        } else {
            // 如果 master 节点不是自己
            
            // 2. 关闭通知客户端失败检查重发的定时器
            //            feedbackJobSendChecker.stop();
            executingDeadJobChecker.stop();
            executableDeadJobChecker.stop();
        }
    }
}
