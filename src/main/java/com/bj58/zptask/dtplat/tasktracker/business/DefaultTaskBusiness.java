package com.bj58.zptask.dtplat.tasktracker.business;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.annotation.Business;
import com.bj58.zptask.dtplat.tasktracker.Result;

/**
 * 默认的任务实现类
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月19日 下午3:59:35
 * @see 
 * @since
 */
@Business
public class DefaultTaskBusiness implements TaskBusiness {

    private static final Logger logger = Logger.getLogger(DefaultTaskBusiness.class);

    @Override
    public Result run() throws Throwable {
        BusinessResult result = new BusinessResult();
        result.setCode(ResultCode.SUCCESS.value());
        result.setMsg("Test In Test.");
        return null;
    }

    @Override
    public void init() throws Throwable {
        logger.info("init ");
    }

}
