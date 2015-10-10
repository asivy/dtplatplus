package com.bj58.zptask.dtplat.jobtracker.registry;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.core.listener.MasterChangeListener;
import com.google.inject.Singleton;

/**
 * job节点master关系发生变化时  的业务处理
 * 应该是可以替换成ZK的master选举机制
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午7:51:38
 * @see 
 * @since
 */
@Singleton
public class JobMasterChangeListener implements MasterChangeListener {

    private static final Logger logger = Logger.getLogger(JobMasterChangeListener.class);

    @Override
    public void change(Node node, boolean isMaster) {
        logger.info(String.format("%s %s  node %s is %s", node.getNodeType(), node.getGroup(), node.getIdentity(), isMaster));
        ExecutingTaskCheck executingTaskCheck = InjectorHolder.getInstance(ExecutingTaskCheck.class);
        ExecutableTaskCheck executableTaskCheck = InjectorHolder.getInstance(ExecutableTaskCheck.class);
        Config config = InjectorHolder.getInstance(Config.class);
        try {
            if (config.getIdentity().equals(node.getIdentity())) {
                executingTaskCheck.start();
                executableTaskCheck.start();
            } else {
                executableTaskCheck.stop();
                executingTaskCheck.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
