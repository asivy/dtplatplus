//package com.bj58.zptask.dtplat.test.example;
//
//import org.apache.log4j.PropertyConfigurator;
//
//import com.bj58.spat.scf.client.SCFInit;
//import com.bj58.zptask.dtplat.commons.InjectorHolder;
//import com.bj58.zptask.dtplat.tasktracker.TaskTracker;
//import com.bj58.zptask.dtplat.test.example.support.MasterChangeListenerImpl;
//import com.bj58.zptask.dtplat.test.example.support.TestJobRunner;
//
///**
// * @author Robert HG (254963746@qq.com) on 8/19/14.
// */
//public class TaskTrackerTest {
//    
//    public static void main(String[] args) {
//        try {
//            PropertyConfigurator.configure("D:/log4j.properties");
//            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
//            InjectorHolder.init();
//            
//            final TaskTracker taskTracker = new TaskTracker();
//            // 任务执行类，实现JobRunner 接口
//            taskTracker.setJobRunnerClass(TestJobRunner.class);
//            taskTracker.setRegistryAddress("zookeeper://192.168.120.189:2181");
//            // taskTracker.setRegistryAddress("redis://127.0.0.1:6379");
//            taskTracker.setNodeGroup("track_group"); // 同一个TaskTracker集群这个名字相同
//            taskTracker.setIdentity("task-tracker-5");
//            taskTracker.setWorkThreads(20);
//            // 反馈任务给JobTracker失败，存储本地文件路径
//            // taskTracker.setFailStorePath(Constants.USER_HOME);
//            // master 节点变化监听器，当有集群中只需要一个节点执行某个事情的时候，可以监听这个事件
//            taskTracker.addMasterChangeListener(new MasterChangeListenerImpl());
//            // 业务日志级别
//            // taskTracker.setBizLoggerLevel(Level.INFO);
//            // 可选址  leveldb(默认), rocksdb, bekeleydb
//            // taskTracker.addConfig("job.fail.store", "leveldb");
//            taskTracker.start();
//
//            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    taskTracker.stop();
//                }
//            }));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}