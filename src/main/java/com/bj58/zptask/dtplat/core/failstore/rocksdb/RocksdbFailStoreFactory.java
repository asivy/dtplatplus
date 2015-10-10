package com.bj58.zptask.dtplat.core.failstore.rocksdb;

import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.failstore.FailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreFactory;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 * Robert HG (254963746@qq.com) on 5/27/15.
 */
public class RocksdbFailStoreFactory implements FailStoreFactory {

    @Override
    public FailStore getFailStore(Config config, String storePath) {
        if (StringUtils.isEmpty(storePath)) {
            storePath = config.getFailStorePath();
        }
        return new RocksdbFailStore(storePath, config.getIdentity());
    }
}
