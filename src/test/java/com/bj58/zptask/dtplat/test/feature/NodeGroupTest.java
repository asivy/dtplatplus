package com.bj58.zptask.dtplat.test.feature;

import java.util.List;

import org.junit.Test;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.zhaopin.feature.contract.INodeGroupService;
import com.bj58.zhaopin.feature.entity.NodeGroupBean;

public class NodeGroupTest {

    static INodeGroupService service;
    static {
        try {
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            service = ProxyFactory.create(INodeGroupService.class, "tcp://feature/NodeGroupServiceImpl");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void load() {
        try {
            List<NodeGroupBean> list = service.loadByType("TASK_TRACKER");
            if (list != null) {
                System.out.println(list.toString());
            }
            System.out.println("finish");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void loadByTypeName() {
        try {
            NodeGroupBean bean = service.loadByTypeName("TASK_TRACKER", "test");
            System.out.println(bean.toString());
            System.out.println("finish");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
