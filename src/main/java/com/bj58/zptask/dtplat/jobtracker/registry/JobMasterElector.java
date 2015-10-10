package com.bj58.zptask.dtplat.jobtracker.registry;

import java.io.Closeable;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.util.Constants;

/**
 * 负责管理JOB的Master选举
 * 替换MasterElector 类
 * 当失去领导权时  应该怎样调用
 * 怎样防止脑裂现象
 * 
 * 
 * @author WuTong 
 * @version 1.0
 * @date  2015年9月17日 上午11:49:53
 * @see 
 * @since
 */
public class JobMasterElector extends LeaderSelectorListenerAdapter implements Closeable {

    private static final Logger logger = Logger.getLogger(JobMasterElector.class);

    private final LeaderSelector leaderSelector;
    ExecutingTaskCheck executingTaskCheck = InjectorHolder.getInstance(ExecutingTaskCheck.class);
    ExecutableTaskCheck executableTaskCheck = InjectorHolder.getInstance(ExecutableTaskCheck.class);

    public JobMasterElector(CuratorFramework client, String node) {
        leaderSelector = new LeaderSelector(client, getRealPath(node), this);
        leaderSelector.autoRequeue();
    }

    /**
     * 成为leader后会调用此方法
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        Config config = InjectorHolder.getInstance(Config.class);
        logger.info(String.format("%s%s-%s take leader ship", Constants.LOGTIP, config.getNodeGroup(), config.getIdentity()));
        executingTaskCheck.start();
        executableTaskCheck.start();
        Thread.sleep(Integer.MAX_VALUE);
    }

    private static String getRealPath(String node) {
        return String.format("/%s/%s/%s", InjectorHolder.getInstance(Config.class).getClusterName(), "Electors", node);
    }

    public void clear() throws Exception {
        executingTaskCheck.stop();
        executableTaskCheck.stop();
    }

    public void start() throws Exception {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
        try {
            clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {//失去leader关系
            try {
                Config config = InjectorHolder.getInstance(Config.class);
                logger.info(String.format("%s%s-%s lose leader ship", Constants.LOGTIP, config.getNodeGroup(), config.getIdentity()));
                clear();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
