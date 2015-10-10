package com.bj58.zptask.dtplat.test.feature;

import java.util.Date;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.zhaopin.feature.contract.ITaskExecutableService;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;

public class TaskExecutableTest {

    static ITaskExecutableService service;
    static {
        try {
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            service = ProxyFactory.create(ITaskExecutableService.class, "tcp://feature/TaskExecutableServiceImpl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void add() {
        try {
            for (int i = 0; i < 1; i++) {
                TaskExecutableBean define = new TaskExecutableBean();
                define.setCreateDate(new Date());
                define.setCronExpression("0 0/1 * * * ?");
                define.setJobId(1441019325805l);
                define.setModifyDate(new Date());
                define.setTriggerDate(new Date());
                define.setPriority(11);
                define.setRetryTimes(2);
                define.setRunning(false);
                define.setSubmitGroup("test_group");
                define.setTaskId("6");
                define.setTaskIdentity("task-tracker-5");
                define.setTaskGroup("track_group");

                service.insert(define);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void load() {
        try {
            TaskExecutableBean task = service.loadOneByGroupRunningTrigger("", false, new Date());
            System.out.println(JSON.toJSON(task));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
        try {
            service.deleteByJobID(1234123123l, "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
