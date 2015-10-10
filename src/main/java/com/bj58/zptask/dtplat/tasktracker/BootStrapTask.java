package com.bj58.zptask.dtplat.tasktracker;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelManager;
import com.bj58.zptask.dtplat.rpc.netty.NettyClientConfig;
import com.bj58.zptask.dtplat.tasktracker.business.TaskPullExecutor;
import com.bj58.zptask.dtplat.tasktracker.registry.TaskRegistryManager;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.zookeeper.CuratorZKClient;

/**
 * 任务的主入口类
 * 这个对接主要的业务  需要提供外部切入的入口 
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午2:13:31
 * @see 
 * @since
 */
public class BootStrapTask {

    private static final Logger logger = Logger.getLogger(BootStrapTask.class);

    private NettyClientConfig clientConfig;
    private Config config;

    public static void main(String[] args) {
        final BootStrapTask task = new BootStrapTask();
        task.start();
    }

    private void start() {
        try {
            initConfig();//初始化配置
            initRegistry();//启动注册服务
            initCheck();//启动后台检查
            startNettyClient();//启动此服务
            startBusiness();
            addhook();
            logger.info(String.format("%sTask Client Start Success with identity : %s", Constants.LOGTIP, config.getIdentity()));
        } catch (Exception e) {
            try {
                stopAll();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        } finally {
            logger.info("");
            logger.info(String.format("%sMay Job Be With You.%s", Constants.LOGTIP, Constants.LOGTIP));
        }
    }

    /**
     * 
     * 初始化配置文件
     * 初始化系统配置serverconfig
     * 初始化业务配置config
     */
    final void initConfig() throws Exception {
        PropertyConfigurator.configure("D:/log4j.properties");
        SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
        InjectorHolder.init();
        logger.info("");
        clientConfig = InjectorHolder.getInstance(NettyClientConfig.class);
        config = InjectorHolder.getInstance(Config.class);
        config.setIdentity("task-demo-3");
        config.setNodeType(NodeType.TASK_TRACKER);
        InjectorHolder.getInstance(CuratorZKClient.class);
        Thread.sleep(3000);
        logger.info(String.format("%sInit Config Success%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    /**
     * 1 实现ZK的注册中心
     * 2 监听每个节点的变化
     * 3 监听同类别节点主从的变化
     * 4 清空僵尸任务  只能有
     * @throws Exception
     */
    final void initRegistry() throws Exception {
        InjectorHolder.getInstance(TaskRegistryManager.class).start();
        logger.info(String.format("%sInit Registry Success%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    /**
     * 执行后台检查任务
     * 这个是任务一个Server都需要做的
     */
    final public void initCheck() throws Exception {
        InjectorHolder.getInstance(ChannelManager.class).start();
        logger.info(String.format("%sInit Check Success%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    /**
     * 启动服务
     */
    final public void startNettyClient() throws Exception {
        NettyClientProxy proxy = InjectorHolder.getInstance(NettyClientProxy.class);
        proxy.init();
        logger.info(String.format("%sStart Netty Success%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    /**
     * 周期性拉到任务 并执行的执行
     * @throws Exception
     */
    final private void startBusiness() throws Exception {
        TaskPullExecutor taskExecutor = InjectorHolder.getInstance(TaskPullExecutor.class);
        taskExecutor.start();
    }

    final public void addhook() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    stopAll();
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        }));
        logger.info(String.format("%sAdd Stophook %s", Constants.LOGTIP, Constants.LOGTIP));
    }

    final public void stopAll() throws Exception {

        InjectorHolder.getInstance(TaskPullExecutor.class).stop();
        logger.info(String.format("Task Client Stopped "));
    }

}
