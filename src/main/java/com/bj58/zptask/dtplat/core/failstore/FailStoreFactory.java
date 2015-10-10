package com.bj58.zptask.dtplat.core.failstore;

import com.bj58.zptask.dtplat.core.cluster.Config;

/**
 * Robert HG (254963746@qq.com) on 5/21/15.
 */
public interface FailStoreFactory {

    public FailStore getFailStore(Config config, String storePath);

}
