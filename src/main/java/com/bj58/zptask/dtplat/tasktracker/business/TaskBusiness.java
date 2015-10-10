package com.bj58.zptask.dtplat.tasktracker.business;

import com.bj58.zptask.dtplat.tasktracker.Result;

/**
 * 所有待执行的业务逻辑 都需要继续此接口
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月19日 下午2:06:14
 * @see 
 * @since
 */
public interface TaskBusiness {

    public abstract void init() throws Throwable;

    public abstract Result run() throws Throwable;

}
