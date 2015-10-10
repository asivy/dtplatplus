package com.bj58.zptask.dtplat.jobtracker;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelManager;
import com.bj58.zptask.dtplat.jobtracker.registry.JobRegistryManager;
import com.bj58.zptask.dtplat.rpc.netty.NettyServerConfig;
import com.bj58.zptask.dtplat.rpc.netty5.NettyServer;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.zookeeper.CuratorZKClient;

/**
 * JobTracker入口类
 * 此类可以直接运行  无需个性化开发
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 上午11:12:58
 * @see 
 * @since
 */
public class BootStrapJob {

    private static final Logger logger = Logger.getLogger(BootStrapJob.class);

    private NettyServerConfig serverConfig;
    private Config config;

    public static void main(String[] args) {
        final BootStrapJob main = new BootStrapJob();
        main.start();
    }

    private void start() {
        try {
            initConfig();//初始化配置
            initRegistry();//启动注册服务
            initCheck();//启动后台检查
            startNettyServer();//启动此服务
            addhook();//添加关闭勾子
            logger.info("");
            logger.info(String.format("%sJob Server Start Success with port = %d", Constants.LOGTIP, serverConfig.getListenPort()));
        } catch (Exception e) {
            e.printStackTrace();
            //启动失败 记得关闭已启服务
            try {
                stopAll();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        } finally {
            try {
                Thread.sleep(3000);
                logger.info("");
                logger.info(String.format("%sUse It Well.%s", Constants.LOGTIP, Constants.LOGTIP));
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        int listenport = 6062;
        serverConfig = InjectorHolder.getInstance(NettyServerConfig.class);
        config = InjectorHolder.getInstance(Config.class);
        serverConfig.setListenPort(listenport);
        config.setListenPort(listenport);

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
        InjectorHolder.getInstance(JobRegistryManager.class).start();
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
     * 启动NETTY服务端
     */
    final public void startNettyServer() throws Exception {
        InjectorHolder.getInstance(NettyServer.class).bind(serverConfig.getListenPort());
        logger.info(String.format("%sStart Netty Success%s", Constants.LOGTIP, Constants.LOGTIP));
    }

    /**
     * 停止
     */
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
        InjectorHolder.getInstance(NettyServer.class).shutdown();
        InjectorHolder.getInstance(ChannelManager.class).stop();
        InjectorHolder.getInstance(JobRegistryManager.class).stop();
        logger.info(String.format("Job Server Stopped "));
    }
}
