package com.bj58.zptask.dtplat.test.core.spi;

import com.bj58.zptask.dtplat.core.Application;

/**
 * @author Robert HG (254963746@qq.com) on 5/18/15.
 */
public class MainTest {

    public static void main(String[] args) {

        //        TestService testService = ExtensionLoader.getExtensionLoader(TestService.class).getExtension("test2");
        //        TestService testService = ExtensionLoader.getExtensionLoader(TestService.class).getAdaptiveExtension();
        //        TestService testService2 = ExtensionLoader.getExtensionLoader(TestService.class).getAdaptiveExtension();
        //        TestService testService3 = ExtensionLoader.getExtensionLoader(TestService.class).getAdaptiveExtension();
        //        TestService testService4 = ExtensionLoader.getExtensionLoader(TestService.class).getAdaptiveExtension();
        //        Config config = new Config();
        //        config.setParameter("test.type", "test2");
        //        testService.sayHello(config);
        Application application = new Application() {
        };
    }

}
