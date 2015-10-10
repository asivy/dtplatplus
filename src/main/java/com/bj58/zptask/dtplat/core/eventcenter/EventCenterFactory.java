package com.bj58.zptask.dtplat.core.eventcenter;

import com.bj58.zptask.dtplat.core.cluster.Config;

/**
 * @author Robert HG (254963746@qq.com) on 5/19/15.
 */
public interface EventCenterFactory {

    EventCenter getEventCenter(Config config);

}
