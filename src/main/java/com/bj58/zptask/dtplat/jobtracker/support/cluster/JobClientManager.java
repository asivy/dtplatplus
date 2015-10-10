package com.bj58.zptask.dtplat.jobtracker.support.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.core.loadbalance.LoadBalance;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.jobtracker.channel.ChannelWrapper;
import com.bj58.zptask.dtplat.jobtracker.domain.JobClientNode;
import com.bj58.zptask.dtplat.jobtracker.domain.JobTrackerApplication;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.ConcurrentHashSet;

/**
 *         客户端节点管理
 */
public class JobClientManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobClientManager.class);

    private final ConcurrentHashMap<String/*nodeGroup*/, Set<JobClientNode>> NODE_MAP = new ConcurrentHashMap<String, Set<JobClientNode>>();

    private LoadBalance loadBalance;
    private JobTrackerApplication application;

    public JobClientManager(JobTrackerApplication application) {
        this.application = application;
        this.loadBalance = InjectorHolder.getInstance(LoadBalance.class);
    }

    /**
     * get all connected node group
     *
     * @return
     */
    public Set<String> getNodeGroups() {
        return NODE_MAP.keySet();
    }

    /**
     * 添加节点
     *
     * @param node
     */
    public void addNode(Node node) {
        //  channel 可能为 null
        ChannelWrapper channel = application.getChannelManager().getChannel(node.getGroup(), node.getNodeType(), node.getIdentity());
        Set<JobClientNode> jobClientNodes = NODE_MAP.get(node.getGroup());

        if (jobClientNodes == null) {
            jobClientNodes = new ConcurrentHashSet<JobClientNode>();
            Set<JobClientNode> oldSet = NODE_MAP.putIfAbsent(node.getGroup(), jobClientNodes);
            if (oldSet != null) {
                jobClientNodes = oldSet;
            }
        }

        JobClientNode jobClientNode = new JobClientNode(node.getGroup(), node.getIdentity(), channel);
        LOGGER.info("add JobClient node:{}", jobClientNode);
        jobClientNodes.add(jobClientNode);

        // create feedback queue
        //        application.getJobFeedbackQueue().createQueue(node.getGroup());
        //        application.getNodeGroupStore().addNodeGroup(NodeType.JOB_CLIENT, node.getGroup());

        //启动时   记录一次分组信息
        InjectorHolder.getInstance(DTaskProvider.class).addNodeGroup("JOB_CLIENT", node.getGroup());
    }

    /**
     * 删除节点
     *
     * @param node
     */
    public void removeNode(Node node) {
        Set<JobClientNode> jobClientNodes = NODE_MAP.get(node.getGroup());
        if (jobClientNodes != null && jobClientNodes.size() != 0) {
            for (JobClientNode jobClientNode : jobClientNodes) {
                if (node.getIdentity().equals(jobClientNode.getIdentity())) {
                    LOGGER.info("remove JobClient node:{}", jobClientNode);
                    jobClientNodes.remove(jobClientNode);
                }
            }
        }
    }

    /**
     * 得到 可用的 客户端节点
     *
     * @param nodeGroup
     * @return
     */
    public JobClientNode getAvailableJobClient(String nodeGroup) {

        Set<JobClientNode> jobClientNodes = NODE_MAP.get(nodeGroup);

        if (CollectionUtils.isEmpty(jobClientNodes)) {
            return null;
        }

        List<JobClientNode> list = new ArrayList<JobClientNode>(jobClientNodes);

        while (list.size() > 0) {

            JobClientNode jobClientNode = loadBalance.select(application.getConfig(), list, null);

            if (jobClientNode != null && (jobClientNode.getChannel() == null || jobClientNode.getChannel().isClosed())) {
                ChannelWrapper channel = application.getChannelManager().getChannel(jobClientNode.getNodeGroup(), NodeType.JOB_CLIENT, jobClientNode.getIdentity());
                if (channel != null) {
                    // 更新channel
                    jobClientNode.setChannel(channel);
                }
            }

            if (jobClientNode != null && jobClientNode.getChannel() != null && !jobClientNode.getChannel().isClosed()) {
                return jobClientNode;
            } else {
                list.remove(jobClientNode);
            }
        }
        return null;
    }

}
