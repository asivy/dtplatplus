package com.bj58.zptask.dtplat.test.java;

import java.util.Date;

import org.junit.Test;

import com.bj58.zhaopin.feature.util.DateUtil;
import com.bj58.zptask.dtplat.util.CronExpressionUtils;

public class DateTest {

    @Test
    public void beforetest() {
        System.out.println(DateUtil.fullsdf.format(before(60 * 1000)));
    }

    private Date before(long ms) {
        return new Date(System.currentTimeMillis() - ms);
    }

    @Test
    public void mstest() {
        System.out.println(DateUtil.fullsdf.format(new Date(1441446500000l)));
    }

    @Test
    public void nextcron() {
        try {
            Date d = CronExpressionUtils.getNextTriggerTime("3 * * * * ?");
            System.out.println(DateUtil.fullsdf.format(d));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
