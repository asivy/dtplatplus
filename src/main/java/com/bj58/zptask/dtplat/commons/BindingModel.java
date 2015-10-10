package com.bj58.zptask.dtplat.commons;

import com.bj58.zptask.dtplat.core.compiler.support.AdaptiveCompiler;
import com.bj58.zptask.dtplat.core.eventcenter.EventCenterFactory;
import com.bj58.zptask.dtplat.core.eventcenter.InJvmEventCenterFactory;
import com.bj58.zptask.dtplat.core.failstore.FailStoreFactory;
import com.bj58.zptask.dtplat.core.failstore.leveldb.LeveldbFailStoreFactory;
import com.bj58.zptask.dtplat.core.loadbalance.ConsistentHashLoadBalance;
import com.bj58.zptask.dtplat.core.loadbalance.LoadBalance;
import com.bj58.zptask.dtplat.core.logger.LoggerAdapter;
import com.bj58.zptask.dtplat.core.logger.log4j.Log4jLoggerAdapter;
import com.google.inject.AbstractModule;

public class BindingModel extends AbstractModule {

    @Override
    protected void configure() {
        //        binder().bind(JobLoggerFactory.class).toInstance(new MysqlJobLoggerFactory());
        //        binder().bind(CronJobQueueFactory.class).toInstance(new MysqlCronJobQueueFactory());
        //        binder().bind(ExecutableJobQueueFactory.class).toInstance(new MysqlExecutableJobQueueFactory());
        //        binder().bind(ExecutingJobQueueFactory.class).toInstance(new MysqlExecutingJobQueueFactory());
        //        binder().bind(JobFeedbackQueueFactory.class).toInstance(new MysqlJobFeedbackQueueFactory());
        //        binder().bind(NodeGroupStoreFactory.class).toInstance(new MysqlNodeGroupStoreFactory());
        binder().bind(EventCenterFactory.class).toInstance(new InJvmEventCenterFactory());
        //        binder().bind(IdGenerator.class).toInstance(new TimeIDGenerator());
        binder().bind(LoadBalance.class).toInstance(new ConsistentHashLoadBalance());
        binder().bind(FailStoreFactory.class).toInstance(new LeveldbFailStoreFactory());
        //        binder().bind(ZookeeperTransporter.class).toInstance(new CuratorZookeeperTransporter());
        binder().bind(LoggerAdapter.class).toInstance(new Log4jLoggerAdapter());
        binder().bind(com.bj58.zptask.dtplat.core.compiler.Compiler.class).toInstance(new AdaptiveCompiler());
    }

}
