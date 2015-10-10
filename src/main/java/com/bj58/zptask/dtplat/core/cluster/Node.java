package com.bj58.zptask.dtplat.core.cluster;

import com.bj58.zptask.dtplat.core.support.SystemClock;
import com.bj58.zptask.dtplat.zookeeper.NodePathHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 节点对象
 * 对应JOBTRACKER TASKTRACKER JOBCLIENT 三类
 * @author Robert HG (254963746@qq.com) on 6/22/14. 节点
 */
public class Node {

    private boolean available = true; // 是否可用
    private String clusterName;
    private NodeType nodeType;
    private String ip;
    private Integer port;
    private String group;
    private Long createTime = SystemClock.now();

    private Integer threads;// 线程个数

    private String identity;// 唯一标识

    // 自己关注的节点类型
    private List<String> listenNodeTypes;

    private String fullString;

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean isAvailable) {
        this.available = isAvailable;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Integer getThreads() {
        return threads;
    }

    public void setThreads(Integer threads) {
        this.threads = threads;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }

    public List<String> getListenNodeTypes() {
        return listenNodeTypes;
    }

    public void addListenNodeType(String nodeGroup) {
        if (this.listenNodeTypes == null) {
            this.listenNodeTypes = new ArrayList<String>();
        }
        this.listenNodeTypes.add(nodeGroup);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Node node = (Node) o;

        return !(identity != null ? !identity.equals(node.identity) : node.identity != null);

    }

    @Override
    public int hashCode() {
        return identity != null ? identity.hashCode() : 0;
    }

    public String getAddress() {
        return ip + ":" + port;
    }

    public String toFullString() {
        if (fullString == null) {
            fullString = NodePathHelper.getFullPath(this);
        }
        return fullString;
    }

    @Override
    public String toString() {
        return "Node{" + "identity='" + identity + '\'' + ", clusterName='" + clusterName + '\'' + ", nodeType=" + nodeType + ", ip='" + ip + '\'' + ", port=" + port + ", group='" + group + '\'' + ", createTime=" + createTime + ", threads=" + threads + ", isAvailable=" + available + ", listenNodeTypes=" + listenNodeTypes + '}';
    }
}
