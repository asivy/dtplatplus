package com.bj58.zptask.dtplat.jobtracker;

/**
 * 检测接口类
 * 因为检测操作都是以后台调度服务实现的  
 * 所以   当进程停掉时    一定要停掉线程池正在执行的服务
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月16日 下午8:17:58
 * @see 
 * @since
 */
public interface Damon {

    public abstract void start() throws Exception;

    public abstract void stop() throws Exception;
}
