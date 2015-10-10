//package com.bj58.zptask.dtplat.test.example.support;
//
//import com.bj58.zptask.dtplat.core.domain.Action;
//import com.bj58.zptask.dtplat.core.domain.Job;
//import com.bj58.zptask.dtplat.core.logger.Logger;
//import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
//import com.bj58.zptask.dtplat.tasktracker.Result;
//import com.bj58.zptask.dtplat.tasktracker.logger.BizLogger;
//import com.bj58.zptask.dtplat.tasktracker.runner.JobRunner;
//import com.bj58.zptask.dtplat.tasktracker.runner.LtsLoggerFactory;
//
///**
// * @author Robert HG (254963746@qq.com) on 8/19/14.
// */
//public class TestJobRunner implements JobRunner {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(TestJobRunner.class);
//
//    @Override
//    public Result run(Job job) throws Throwable {
//        try {
//            LOGGER.info("正在执行：" + job);
//            BizLogger bizLogger = LtsLoggerFactory.getBizLogger();
//            // 会发送到 LTS (JobTracker上)
//            bizLogger.info("Test Test " + System.currentTimeMillis());
//        } catch (Exception e) {
//            LOGGER.info("Run job failed!", e);
//            return new Result(Action.EXECUTE_LATER, e.getMessage());
//        }
//        return new Result(Action.EXECUTE_SUCCESS, "执行成功了，哈哈");
//    }
//}
