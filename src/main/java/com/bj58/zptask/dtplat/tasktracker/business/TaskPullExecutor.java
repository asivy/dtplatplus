package com.bj58.zptask.dtplat.tasktracker.business;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.util.StringUtil;
import com.bj58.zptask.dtplat.annotation.Business;
import com.bj58.zptask.dtplat.jobtracker.Damon;
import com.bj58.zptask.dtplat.util.ClassHelper;
import com.bj58.zptask.dtplat.util.GenericsUtils;
import com.bj58.zptask.dtplat.util.PackageScan;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Singleton;

/**
 * 拉取任务  并执行
 * 1 定时任务  每个Task都可以执行
 * 2 实时任务  只有Master节点可以执行    暂不做 
 * 3 任务执行完 一定要返回一个结果  以便在页面中明显的看出来
 * 4 任务的执行入口在 taskdefine.mainClass中定义   一个
 * 
 * 
 * @author WuTong
 * @version 1.0 
 * @date  2015年9月18日 下午3:36:10
 * @see 
 * @since
 */
@Singleton
public class TaskPullExecutor implements Damon {

    private static final Logger logger = Logger.getLogger(TaskPullExecutor.class);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final ConcurrentHashMap<String, Object> businessMap = GenericsUtils.newConcurrentHashMap();
    private final ConcurrentHashMap<String, Class<?>> classMap = GenericsUtils.newConcurrentHashMap();
    private final ReentrantLock initLock = new ReentrantLock();
    private EventBus taskBus;

    @Override
    public void start() throws Exception {
        try {
            findAll();
            taskBus = new EventBus("task");
            taskBus.register(new BusinessExecutor());
            executor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    taskBus.post("DefaultTaskBusiness");
                }
            }, 5, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() throws Exception {
        try {
            executor.shutdown();
        } catch (Exception e) {//防御性捕获
            e.printStackTrace();
        }
    }

    /**
     * 获取所有带business注解的类
     * 要求同一个类名 不有对应同一个Class
     */
    private void findAll() {
        try {
            Set<Class<?>> allClass = PackageScan.scanClasses("com.bj58.zptask");
            for (Class<?> cls : allClass) {
                if (cls.isAnnotationPresent(Business.class)) {
                    String name = cls.getName();
                    classMap.put(name.substring(name.lastIndexOf(".") + 1, name.length()), cls);
                }
            }
        } catch (Exception e) {

        }
    }

    private class BusinessExecutor {
        @Subscribe
        public void doTask(String mainClass) {
            try {
                Class<?> mclass = classMap.get(mainClass);
                Preconditions.checkNotNull(mclass);
                //需保证每一个类只能被初始化一次
                initLock.tryLock(1000, TimeUnit.MILLISECONDS);
                Object obj = businessMap.get(mainClass);
                if (obj == null) {
                    obj = mclass.newInstance();
                    Method init = mclass.getMethod("init");
                    init.invoke(obj);
                    businessMap.put(mainClass, obj);
                }
                Method m = mclass.getMethod("run");
                m.invoke(obj);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                initLock.unlock();
            }
        }

        @Subscribe
        public void doExecutableTask(TaskExecutableBean etask) {
            Preconditions.checkNotNull(etask);
            if (StringUtil.isNullOrEmpty(etask.getMainClass())) {
                return;
            }
            try {
                Class<?> mainclass = ClassHelper.forName(etask.getMainClass());
                if (mainclass.isAssignableFrom(TaskBusiness.class)) {

                    return;
                }

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

}
