package com.bj58.zptask.dtplat.zookeeper;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

/**
 * 本是curator zk两种实现 
 * 现在只改用一种  用好就可以了  用那么多干麻
 */
public interface ZookeeperClient {

    CuratorFramework getClient();

    String create(String path, boolean ephemeral, boolean sequential);

    String create(String path, Object data, boolean ephemeral, boolean sequential);

    boolean delete(String path);

    boolean exists(String path);

    <T> T getData(String path);

    void setData(String path, Object data);

    List<String> getChildren(String path);

    List<String> addChildListener(String path, ChildListener listener);

    void removeChildListener(String path, ChildListener listener);

    void addStateListener(StateListener listener);

    void removeStateListener(StateListener listener);

    boolean isConnected();

    void close();

}
