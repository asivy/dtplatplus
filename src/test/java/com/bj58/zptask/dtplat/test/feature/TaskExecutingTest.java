package com.bj58.zptask.dtplat.test.feature;

import java.util.Date;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.zhaopin.feature.contract.ITaskExecutingService;
import com.bj58.zhaopin.feature.entity.TaskExecutingBean;
import com.bj58.zptask.dtplat.util.SnowFlakeUuid;

public class TaskExecutingTest {

    static ITaskExecutingService service;
    static {
        try {
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            service = ProxyFactory.create(ITaskExecutingService.class, "tcp://feature/TaskExecutingServiceImpl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void add() {
        try {
            for (int i = 0; i < 10; i++) {
                TaskExecutingBean define = new TaskExecutingBean();
                define.setCreateDate(new Date());
                define.setCronExpression("0 0/1 * * * ?");
                define.setJobId(SnowFlakeUuid.getInstance().nextId());
                define.setModifyDate(new Date());
                define.setTriggerDate(new Date());
                define.setPriority(11);
                define.setRetryTimes(2);
                define.setRunning(false);
                define.setSubmitGroup("test_group");
                define.setTaskId("6");
                define.setTaskIdentity("iden");
                define.setTaskGroup("track_group");

                service.insert(define);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
        try {
            service.deleteByJobID(149337587277246464l);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void loadByJOBId() {
        try {
            TaskExecutingBean task = service.loadByJobID(149337587373715456l);
            System.out.println(JSON.toJSON(task));
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
}
