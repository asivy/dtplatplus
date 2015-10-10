package com.bj58.zptask.dtplat.test.core.failstore.rocksdb;

import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.NodeType;
import com.bj58.zptask.dtplat.core.domain.Job;
import com.bj58.zptask.dtplat.core.domain.KVPair;
import com.bj58.zptask.dtplat.core.failstore.FailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreException;
import com.bj58.zptask.dtplat.core.failstore.rocksdb.RocksdbFailStore;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.Constants;
import com.bj58.zptask.dtplat.util.JSONUtils;
import com.bj58.zptask.dtplat.util.StringUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Robert HG (254963746@qq.com) on 5/27/15.
 */
public class RocksdbFailStoreTest {

    FailStore failStore;

    private String key = "23412x";

    @Before
    public void setup() throws FailStoreException {
        Config config = new Config();
        config.setIdentity(StringUtils.generateUUID());
        config.setNodeGroup("berkeleydb_test");
        config.setNodeType(NodeType.JOB_CLIENT);
        config.setFailStorePath(Constants.USER_HOME);
        failStore = new RocksdbFailStore(config.getFailStorePath(), config.getIdentity());
        failStore.open();
    }

    @Test
    public void put() throws FailStoreException {
        Job job = new Job();
        job.setTaskId("2131232");
        for (int i = 0; i < 100; i++) {
            failStore.put(key + "" + i, job);
        }
        System.out.println("这里debug测试多线程");
        failStore.close();
    }

    @Test
    public void fetchTop() throws FailStoreException {
        List<KVPair<String, Job>> kvPairs = failStore.fetchTop(5, Job.class);
        if (CollectionUtils.isNotEmpty(kvPairs)) {
            for (KVPair<String, Job> kvPair : kvPairs) {
                System.out.println(JSONUtils.toJSONString(kvPair));
            }
        }
    }
}