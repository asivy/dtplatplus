package com.bj58.zptask.dtplat.test.java;

import java.text.ParseException;
import java.util.Date;

import org.junit.Test;
import org.quartz.CronExpression;

import com.bj58.zhaopin.feature.util.DateUtil;

public class CronTest {

    @Test
    public void next() {
        try {
            Date now = new Date();
            CronExpression cron = new CronExpression("5 * * * * ?");
            Date d = cron.getNextValidTimeAfter(now);
            System.out.println(DateUtil.fullsdf.format(now));
            System.out.println(DateUtil.fullsdf.format(d));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
