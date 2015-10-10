package com.bj58.zptask.dtplat.jobtracker.registry;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.jobtracker.Damon;
import com.bj58.zptask.dtplat.util.Constants;
import com.google.inject.Singleton;

/**
 * 检查正在执行的任务
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午8:09:17
 * @see 
 * @since
 */
@Singleton
public class ExecutableTaskCheck implements Damon {

    private static final Logger logger = Logger.getLogger(ExecutableTaskCheck.class);

    @Override
    public void start() throws Exception {
        logger.info(String.format("%sExecutable Task Check Start%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    @Override
    public void stop() throws Exception {
        logger.info(String.format("%sExecutable Task Check Stop%s", Constants.LOGTIP, Constants.LOGTIP));

    }

}
