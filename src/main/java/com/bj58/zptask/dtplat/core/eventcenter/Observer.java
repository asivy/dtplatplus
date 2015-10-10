package com.bj58.zptask.dtplat.core.eventcenter;

/**
 * 事件观察者接口
 * 
 * @author Robert HG (254963746@qq.com) on 5/11/15.
 */
public interface Observer {

    public void onObserved(EventInfo eventInfo);

}
