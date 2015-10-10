package com.bj58.zptask.dtplat.core.loadbalance;

import java.util.List;

import com.bj58.zptask.dtplat.core.cluster.Config;

/**
 * Robert HG (254963746@qq.com) on 3/25/15.
 */
public interface LoadBalance {

    public <S> S select(Config config, List<S> shards, String seed);

}
