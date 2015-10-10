package com.bj58.zptask.dtplat.jobtracker.channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.google.inject.Singleton;

/**
 *  管理channel
 *  感觉这个类  放到netty中去比较合适
 */
@Singleton
public class ChannelManager {

    private final Logger logger = Logger.getLogger(ChannelManager.class);
    // 客户端列表 (要保证同一个group的node要是无状态的)
    private final ConcurrentHashMap<String/*clientGroup*/, List<ChannelWrapper>> clientChannelMap = new ConcurrentHashMap<String, List<ChannelWrapper>>();
    // 任务节点列表
    private final ConcurrentHashMap<String/*taskGroup*/, List<ChannelWrapper>> taskTrackerChannelMap = new ConcurrentHashMap<String, List<ChannelWrapper>>();
    // 用来定时检查已经关闭的channel
    private final ScheduledExecutorService channelCheckExecutorService = Executors.newScheduledThreadPool(1);

    private ScheduledFuture scheduledFuture;
    private AtomicBoolean start = new AtomicBoolean(false);

    public void start() {
        try {
            if (start.compareAndSet(false, true)) {
                scheduledFuture = channelCheckExecutorService.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        checkCloseChannel(clientChannelMap);
                        logger.debug("JobClient Channel Pool " + clientChannelMap);
                        checkCloseChannel(taskTrackerChannelMap);
                        logger.debug("TaskTracker Channel Pool " + taskTrackerChannelMap);

                    }
                }, 10, 10, TimeUnit.SECONDS);
            }
            logger.debug("Start channel manager success!");
        } catch (Throwable t) {
            logger.error("Start channel manager failed!", t);
        }
    }

    public void stop() {
        try {
            if (start.compareAndSet(true, false)) {
                scheduledFuture.cancel(true);
                channelCheckExecutorService.shutdown();
            }
            logger.info("Stop channel manager success!");
        } catch (Throwable t) {
            logger.error("Stop channel manager failed!", t);
        }
    }

    /**
     * 检查 关闭的channel
     * 
     * @param channelMap
     */
    private void checkCloseChannel(ConcurrentHashMap<String, List<ChannelWrapper>> channelMap) {
        for (Map.Entry<String, List<ChannelWrapper>> entry : channelMap.entrySet()) {
            List<ChannelWrapper> channels = entry.getValue();
            List<ChannelWrapper> removeList = new ArrayList<ChannelWrapper>();
            for (ChannelWrapper channel : channels) {
                if (channel.isClosed()) {
                    removeList.add(channel);
                    logger.info(String.format("close channel=%s", channel));
                }
            }
            channels.removeAll(removeList);
        }
    }

    public List<ChannelWrapper> getChannels(String nodeGroup, NodeType nodeType) {
        if (nodeType == NodeType.JOB_CLIENT) {
            return clientChannelMap.get(nodeGroup);
        } else if (nodeType == NodeType.TASK_TRACKER) {
            return taskTrackerChannelMap.get(nodeGroup);
        }
        return null;
    }

    /**
     * 根据 节点唯一编号得到 channel
     *
     * @param nodeGroup
     * @param nodeType
     * @param identity
     * @return
     */
    public ChannelWrapper getChannel(String nodeGroup, NodeType nodeType, String identity) {
        List<ChannelWrapper> channelWrappers = getChannels(nodeGroup, nodeType);
        if (channelWrappers != null && channelWrappers.size() != 0) {
            for (ChannelWrapper channelWrapper : channelWrappers) {
                if (channelWrapper.getIdentity().equals(identity)) {
                    return channelWrapper;
                }
            }
        }
        return null;
    }

    /**
     * 添加channel
     *
     * @param channel
     */
    public void offerChannel(ChannelWrapper channel) {
        String nodeGroup = channel.getNodeGroup();
        NodeType nodeType = channel.getNodeType();
        List<ChannelWrapper> channels = getChannels(nodeGroup, nodeType);
        if (channels == null) {
            channels = new ArrayList<ChannelWrapper>();
            if (nodeType == NodeType.JOB_CLIENT) {
                clientChannelMap.put(nodeGroup, channels);
            } else if (nodeType == NodeType.TASK_TRACKER) {
                taskTrackerChannelMap.put(nodeGroup, channels);
            }
            channels.add(channel);
            logger.info(String.format("new connected channel=%s", channel));
        } else {
            if (!channels.contains(channel)) {
                channels.add(channel);
                logger.info(String.format("new connected channel=%s", channel));
            }
        }
    }

    public void removeChannel(ChannelWrapper channel) {
        String nodeGroup = channel.getNodeGroup();
        NodeType nodeType = channel.getNodeType();
        List<ChannelWrapper> channels = getChannels(nodeGroup, nodeType);
        if (channels != null) {
            channels.remove(channel);
            logger.info(String.format("remove channel=%s", channel));

        }
    }
}
