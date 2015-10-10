package com.bj58.zptask.dtplat.test.feature;

import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.spat.scf.server.contract.entity.Out;
import com.bj58.zhaopin.feature.contract.ITaskLogService;
import com.bj58.zhaopin.feature.entity.TaskLogBean;

public class TaskLogTest {

    static ITaskLogService service;
    static {
        try {
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            service = ProxyFactory.create(ITaskLogService.class, "tcp://feature/TaskLogServiceImpl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addMuch() {
        try {
            for (int i = 0; i < 1; i++) {
                TaskLogBean entity = new TaskLogBean();
                entity.setCreateDate(new Date());
                entity.setLogType("inof");
                entity.setSuccess(true);
                entity.setMsg("haha");
                entity.setTaskIdentity("test");
                entity.setLevel("info");
                entity.setJobId(121);
                entity.setTaskId("6");
                entity.setTaskGroup("tasktracker");
                entity.setCronExpression("0 0/1 * * * ?");
                entity.setTriggerTime(new Date());
                entity.setRetryTimes(4);

                long id = service.insert(entity);
                System.out.println("id=" + id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query() {
        try {
            Out total = new Out(0);
            List<TaskLogBean> list = service.loadPageByTaskGroup("", 30, 1, total);
            if (list != null) {
                for (TaskLogBean bean : list) {
                    System.out.println(JSON.toJSONString(bean));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
