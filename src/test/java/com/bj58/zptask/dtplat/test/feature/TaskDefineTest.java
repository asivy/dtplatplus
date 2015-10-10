package com.bj58.zptask.dtplat.test.feature;

import java.util.Date;

import org.junit.Test;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.zhaopin.feature.contract.ITaskDefineService;
import com.bj58.zhaopin.feature.entity.TaskDefineBean;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;

public class TaskDefineTest {

    static ITaskDefineService service;
    static {
        try {
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            service = ProxyFactory.create(ITaskDefineService.class, "tcp://feature/TaskDefineServiceImpl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void add() {
        try {
            TaskDefineBean define = new TaskDefineBean();
            define.setCreateDate(new Date());
            define.setCronExpression("0 0/1 * * * ?");
            define.setJobId(System.currentTimeMillis());
            define.setModifyDate(new Date());
            define.setTriggerDate(new Date());
            define.setPriority(11);
            define.setRetryTimes(2);
            define.setSubmitGroup("test_group");
            define.setTaskId("6");
            define.setTaskIdentity("task-tracker-5");
            define.setTaskGroup("track_group");

            service.insert(define);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void load() {
        try {
            TaskDefineBean define = InjectorHolder.getInstance(DTaskProvider.class).loadTaskDefineByJobID(150717565495418880l);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

}
