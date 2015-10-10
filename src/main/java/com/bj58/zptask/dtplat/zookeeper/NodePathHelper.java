package com.bj58.zptask.dtplat.zookeeper;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 *  这是一个注册中心的管理类
 *  目前只支持ZK 
 *  
 * @author WuTong
 * @version 1.0
 * @date  2015年9月9日 上午11:22:18
 * @see   
 * @since
 */
public class NodePathHelper {

    public static String getNodePath() {
        Config config = InjectorHolder.getInstance(Config.class);
        return "/" + config.getClusterName() + "/Nodes";
    }

    public static String getLockPath(String node) {
        Config config = InjectorHolder.getInstance(Config.class);
        return String.format("/%s/%s/%s", config.getClusterName(), "Locks", node);
    }

    public static String getNodeTypePath(String nodeGroup) {
        return getNodePath() + "/" + nodeGroup;
    }

    /**
     * 不要fullpath 只在一个节点的值 
     * 此外传的是节点的唯一标识符    
     * @param fullPath
     * @return
     */
    public static Node parse(String fullPath) {
        Node node = new Node();
        String[] nodeDir = fullPath.split("/");
        NodeType nodeType = NodeType.valueOf(nodeDir[3]);
        node.setNodeType(nodeType);
        String url = nodeDir[4];

        url = url.substring(nodeType.name().length() + 3);
        String address = url.split("\\?")[0];
        String ip = address.split(":")[0];

        node.setIp(ip);
        if (address.contains(":")) {
            String port = address.split(":")[1];
            if (port != null && !"".equals(port.trim())) {
                node.setPort(Integer.valueOf(port));
            }
        }
        String params = url.split("\\?")[1];

        String[] paramArr = params.split("&");
        for (String paramEntry : paramArr) {
            String key = paramEntry.split("=")[0];
            String value = paramEntry.split("=")[1];
            if ("clusterName".equals(key)) {
                node.setClusterName(value);
            } else if ("group".equals(key)) {
                node.setGroup(value);
            } else if ("threads".equals(key)) {
                node.setThreads(Integer.valueOf(value));
            } else if ("identity".equals(key)) {
                node.setIdentity(value);
            } else if ("createTime".equals(key)) {
                node.setCreateTime(Long.valueOf(value));
            } else if ("isAvailable".equals(key)) {
                node.setAvailable(Boolean.valueOf(value));
            }
        }
        return node;
    }

    /**
     * 原解析当中 带有各种类型 信息
     * 但这些都是多余的 
     * 因为路径中就含有这些信息了
     * @param node
     * @return
     */
    public static String getFullPath(Node node) {
        StringBuilder path = new StringBuilder();
        
        //父节点路径
        path.append(getNodePath()).append("/").append(node.getNodeType()).append("/");
        //当前节点
        path.append(node.getNodeType()).append(":\\\\").append(node.getIp());

        if (node.getPort() != null && node.getPort() != 0) {
            path.append(":").append(node.getPort());
        }

        path.append("?").append("group=").append(node.getGroup()).append("&clusterName=").append(node.getClusterName());
        if (node.getThreads() != 0) {
            path.append("&threads=").append(node.getThreads());
        }

        path.append("&identity=").append(node.getIdentity()).append("&createTime=").append(node.getCreateTime()).append("&isAvailable=").append(node.isAvailable());
        return path.toString();
    }

    public static String getRealRegistryAddress(String registryAddress) {
        if (StringUtils.isEmpty(registryAddress)) {
            throw new IllegalArgumentException("registryAddress is null！");
        }
        if (registryAddress.startsWith("zookeeper://")) {
            return registryAddress.replace("zookeeper://", "").trim();
        }
        throw new IllegalArgumentException("illegal address protocol");
    }

}
