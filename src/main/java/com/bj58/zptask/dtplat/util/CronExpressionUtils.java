package com.bj58.zptask.dtplat.util;

import java.text.ParseException;
import java.util.Date;

import org.quartz.CronExpression;

import com.bj58.zptask.dtplat.exception.CronException;

/**
 * @author Robert HG (254963746@qq.com) on 5/27/15.
 */
public class CronExpressionUtils {

    private CronExpressionUtils() {
    }

    public static Date getNextTriggerTime(String cronExpression) {
        try {
            CronExpression cron = new CronExpression(cronExpression);
            return cron.getNextValidTimeAfter(new Date());
        } catch (ParseException e) {
            throw new CronException(e);
        }
    }

    public static boolean isValidExpression(String cronExpression) {
        return CronExpression.isValidExpression(cronExpression);
    }

}
