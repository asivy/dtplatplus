package com.bj58.zptask.dtplat.test.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkPool {

    public static final RetryPolicy policy = new ExponentialBackoffRetry(100, 2);
    public static CuratorFramework client = null;

    public static String connectings = "192.168.120.189:2181";

    public static CuratorFramework getClient() {
        try {
            if (client == null) {
                client = CuratorFrameworkFactory.builder().connectString(connectings).sessionTimeoutMs(1000).retryPolicy(policy).build();
                client.start();
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 使用新的zk配置
     */
    public static void distroy() {
        client = null;
    }

}
