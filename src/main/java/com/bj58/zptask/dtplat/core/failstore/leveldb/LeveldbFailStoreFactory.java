package com.bj58.zptask.dtplat.core.failstore.leveldb;

import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.failstore.FailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreFactory;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 * Robert HG (254963746@qq.com) on 5/21/15.
 */
public class LeveldbFailStoreFactory implements FailStoreFactory {
    @Override
    public FailStore getFailStore(Config config, String storePath) {
        if (StringUtils.isEmpty(storePath)) {
            storePath = config.getFailStorePath();
        }
        return new LeveldbFailStore(storePath, config.getIdentity());
    }
}
