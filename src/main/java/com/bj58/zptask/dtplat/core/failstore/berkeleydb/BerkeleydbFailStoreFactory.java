package com.bj58.zptask.dtplat.core.failstore.berkeleydb;

import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.failstore.FailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreFactory;
import com.bj58.zptask.dtplat.util.StringUtils;

/**
 * Robert HG (254963746@qq.com) on 5/26/15.
 */
public class BerkeleydbFailStoreFactory implements FailStoreFactory {
    @Override
    public FailStore getFailStore(Config config, String storePath) {
        if (StringUtils.isEmpty(storePath)) {
            storePath = config.getFailStorePath();
        }
        return new BerkeleydbFailStore(storePath, config.getIdentity());
    }
}
