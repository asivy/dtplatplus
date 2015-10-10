package com.bj58.zptask.dtplat.test.feature;

import java.util.Date;

import org.junit.Test;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.zhaopin.feature.contract.ITaskExecutableService;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;

public class ExecutableTaskTest {
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
    public void testadd() {
        try {
            for (int i = 0; i < 100; i++) {
                TaskExecutableBean task = new TaskExecutableBean();
                task.setCreateDate(new Date());
                task.setCronExpression("0 0/1 * * * ?");
                task.setExtParams("");
                task.setJobId(System.currentTimeMillis());
                task.setModifyDate(new Date());
                task.setTriggerDate(new Date());
                task.setPriority(10);
                task.setExtParams("");
                task.setRetryTimes(3);
                task.setRunning(false);
                task.setSubmitGroup("test");
                task.setTaskId("task" + System.currentTimeMillis());
                task.setTaskIdentity("adsfadsf");
                task.setTaskGroup("group");
                long id = service.insert(task);
                System.out.println(id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
