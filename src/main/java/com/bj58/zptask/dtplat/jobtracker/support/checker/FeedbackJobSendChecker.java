//package com.bj58.zptask.dtplat.jobtracker.support.checker;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import com.bj58.zhaopin.feature.entity.TaskFeedbackBean;
//import com.bj58.zptask.dtplat.commons.DTaskProvider;
//import com.bj58.zptask.dtplat.commons.InjectorHolder;
//import com.bj58.zptask.dtplat.core.domain.JobFeedbackPo;
//import com.bj58.zptask.dtplat.core.domain.TaskTrackerJobResult;
//import com.bj58.zptask.dtplat.core.logger.Logger;
//import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
//import com.bj58.zptask.dtplat.jobtracker.domain.JobClientNode;
//import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
//import com.bj58.zptask.dtplat.jobtracker.support.ClientNotifier;
//import com.bj58.zptask.dtplat.jobtracker.support.ClientNotifyHandler;
//import com.bj58.zptask.dtplat.util.CollectionUtils;
//
///**
// * @author Robert HG (254963746@qq.com) on 8/25/14.
// *         用来检查 执行完成的任务, 发送给客户端失败的 由master节点来做
// *         单利
// */
//public class FeedbackJobSendChecker {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(FeedbackJobSendChecker.class);
//
//    private ScheduledExecutorService RETRY_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
//    private ScheduledFuture scheduledFuture;
//    private AtomicBoolean start = new AtomicBoolean(false);
//    private ClientNotifier clientNotifier;
//    private JobTrackerApplication application;
//
//    /**
//     * 是否已经启动
//     *
//     * @return
//     */
//    private boolean isStart() {
//        return start.get();
//    }
//
//    public FeedbackJobSendChecker(final JobTrackerApplication application) {
//        this.application = application;
//
//        clientNotifier = new ClientNotifier(application, new ClientNotifyHandler<TaskTrackerJobResultWrapper>() {
//            @Override
//            public void handleSuccess(List<TaskTrackerJobResultWrapper> jobResults) {
//                for (TaskTrackerJobResultWrapper jobResult : jobResults) {
//                    String submitNodeGroup = jobResult.getJobWrapper().getJob().getSubmitNodeGroup();
//                    //                    application.getJobFeedbackQueue().remove(submitNodeGroup, jobResult.getId());
//                    InjectorHolder.getInstance(DTaskProvider.class).deleteFeedback(submitNodeGroup, Long.parseLong(jobResult.getId()));
//                }
//            }
//
//            @Override
//            public void handleFailed(List<TaskTrackerJobResultWrapper> jobResults) {
//            }
//        });
//    }
//
//    /**
//     * 启动
//     */
//    public void start() {
//        try {
//            if (start.compareAndSet(false, true)) {
//                scheduledFuture = RETRY_EXECUTOR_SERVICE.scheduleWithFixedDelay(new Runner(), 30, 30, TimeUnit.SECONDS);
//            }
//            LOGGER.info("feedback job checker started!");
//
//        } catch (Throwable t) {
//            LOGGER.error("feedback job checker start failed!", t);
//        }
//    }
//
//    /**
//     * 停止
//     */
//    public void stop() {
//        try {
//            if (start.compareAndSet(true, false)) {
//                scheduledFuture.cancel(true);
//                RETRY_EXECUTOR_SERVICE.shutdown();
//                LOGGER.info("feedback job checker stopped!");
//            }
//        } catch (Throwable t) {
//            LOGGER.error("feedback job checker stop failed!", t);
//        }
//    }
//
//    private volatile boolean isRunning = false;
//
//    private class Runner implements Runnable {
//        @Override
//        public void run() {
//            try {
//                if (isRunning) {
//                    return;
//                }
//                isRunning = true;
//
//                Set<String> taskTrackerNodeGroups = application.getJobClientManager().getNodeGroups();
//                if (CollectionUtils.isEmpty(taskTrackerNodeGroups)) {
//                    return;
//                }
//
//                for (String taskTrackerNodeGroup : taskTrackerNodeGroups) {
//                    check(taskTrackerNodeGroup);
//                }
//
//            } catch (Throwable t) {
//                LOGGER.error(t.getMessage(), t);
//            } finally {
//                isRunning = false;
//            }
//        }
//
//        private void check(String jobClientNodeGroup) {
//
//            // check that node group job client
//            JobClientNode jobClientNode = application.getJobClientManager().getAvailableJobClient(jobClientNodeGroup);
//            if (jobClientNode == null) {
//                return;
//            }
//
//            //            long count = application.getJobFeedbackQueue().getCount(jobClientNodeGroup);
//            long count = InjectorHolder.getInstance(DTaskProvider.class).getCountFeedback(jobClientNodeGroup);
//            if (count == 0) {
//                return;
//            }
//
//            LOGGER.info("{} jobs need to feedback.", count);
//            // 检测是否有可用的客户端
//
//            List<JobFeedbackPo> jobFeedbackPos;
//            List<TaskFeedbackBean> feedbacks;
//            int limit = 5;
//            //整个都需要修改
//            do {
//                feedbacks = InjectorHolder.getInstance(DTaskProvider.class).loadTopFeedback(jobClientNodeGroup, limit);
//                if (CollectionUtils.isEmpty(feedbacks)) {
//                    return;
//                }
//                List<TaskTrackerJobResultWrapper> jobResults = new ArrayList<TaskTrackerJobResultWrapper>(feedbacks.size());
//                for (TaskFeedbackBean feedback : feedbacks) {
//                    // 判断是否是过时的数据，如果是，那么移除
//                    if (application.getOldDataHandler() == null || (!application.getOldDataHandler().handle(feedback, feedback))) {
//                        //                        jobResults.add(new TaskTrackerJobResultWrapper(feedback.getID(), feedback.getResult());
//                    }
//                }
//                // 返回发送成功的个数
//                int sentSize = clientNotifier.send(jobResults);
//
//                LOGGER.info("send to client: {} success, {} failed.", sentSize, jobResults.size() - sentSize);
//            } while (feedbacks.size() > 0);
//
//            //            do {
//            //                jobFeedbackPos = application.getJobFeedbackQueue().fetchTop(jobClientNodeGroup, limit);
//            //                if (CollectionUtils.isEmpty(jobFeedbackPos)) {
//            //                    return;
//            //                }
//            //                List<TaskTrackerJobResultWrapper> jobResults = new ArrayList<TaskTrackerJobResultWrapper>(jobFeedbackPos.size());
//            //                for (JobFeedbackPo jobFeedbackPo : jobFeedbackPos) {
//            //                    // 判断是否是过时的数据，如果是，那么移除
//            //                    if (application.getOldDataHandler() == null || (!application.getOldDataHandler().handle(application.getJobFeedbackQueue(), jobFeedbackPo, jobFeedbackPo))) {
//            //                        jobResults.add(new TaskTrackerJobResultWrapper(jobFeedbackPo.getId(), jobFeedbackPo.getTaskTrackerJobResult()));
//            //                    }
//            //                }
//            //                // 返回发送成功的个数
//            //                int sentSize = clientNotifier.send(jobResults);
//            //
//            //                LOGGER.info("send to client: {} success, {} failed.", sentSize, jobResults.size() - sentSize);
//            //            } while (jobFeedbackPos.size() > 0);
//        }
//    }
//
//    private class TaskTrackerJobResultWrapper extends TaskTrackerJobResult {
//        private String id;
//
//        public String getId() {
//            return id;
//        }
//
//        public TaskTrackerJobResultWrapper(String id, TaskTrackerJobResult result) {
//            this.id = id;
//            setJobWrapper(result.getJobWrapper());
//            setMsg(result.getMsg());
//            setAction(result.getAction());
//            setTime(result.getTime());
//        }
//    }
//
//}
