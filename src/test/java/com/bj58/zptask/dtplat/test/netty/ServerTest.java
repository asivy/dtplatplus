package com.bj58.zptask.dtplat.test.netty;

import org.apache.log4j.PropertyConfigurator;

import com.bj58.spat.scf.client.SCFInit;
import com.bj58.zptask.dtplat.commons.DTaskProvider;
import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.rpc.netty5.NettyServer;

public class ServerTest {

    public static void main(String[] args) {
        try {
            PropertyConfigurator.configure("D:/log4j.properties");
            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
            InjectorHolder.init();
            InjectorHolder.getInstance(DTaskProvider.class).loadTaskDefineByJobID(1);
            NettyServer server = new NettyServer();
            server.bind(6061);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
