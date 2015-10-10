package com.bj58.zptask.dtplat.test.example;

import org.apache.log4j.PropertyConfigurator;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.jobtracker.JobTracker;
import com.bj58.zptask.dtplat.test.example.support.MasterChangeListenerImpl;

/**
 * @author Robert HG (254963746@qq.com) on 8/13/14.
 */
public class JobTrackerTest {

    public static void main(String[] args) {
        try {
            PropertyConfigurator.configure("D:/log4j.properties");
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            InjectorHolder.init();
            testMysqlQueue();
            InjectorHolder.getInstance(DTaskProvider.class).loadTaskDefineByJobID(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 使用mysql做任务队列
     */
    public static void testMysqlQueue() {
        final JobTracker jobTracker = new JobTracker();
        // 节点信息配置
        jobTracker.setRegistryAddress("zookeeper://192.168.120.189:2181");
        //        jobTracker.setRegistryAddress("redis://127.0.0.1:6379");
        jobTracker.setListenPort(35008); // 默认 35001
        jobTracker.setIdentity("job-tracker-5");
        jobTracker.addMasterChangeListener(new MasterChangeListenerImpl());

        // 设置业务日志记录 mysql
        jobTracker.addConfig("job.logger", "mysql");
        // 任务队列用mysql 
        jobTracker.addConfig("job.queue", "mysql");
        // mysql 配置
        jobTracker.addConfig("jdbc.url", "jdbc:mysql://192.168.120.73:3306/dbwww58com_distask");
        jobTracker.addConfig("jdbc.username", "root");
        jobTracker.addConfig("jdbc.password", "zhaopin5678"); 
        
        //        jobTracker.setOldDataHandler(new OldDataDeletePolicy());
        // 设置 zk 客户端用哪个， 可选 zkclient, curator 默认是 zkclient
        //        jobTracker.addConfig("zk.client", "zkclient");
        // 启动节点
        jobTracker.start();
        Integer.parseInt("S");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                jobTracker.stop();
            }
        }));
    }

}
