package com.bj58.zptask.dtplat.core.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.Application;
import com.bj58.zptask.dtplat.core.domain.KVPair;
import com.bj58.zptask.dtplat.core.eventcenter.EventInfo;
import com.bj58.zptask.dtplat.core.eventcenter.EventSubscriber;
import com.bj58.zptask.dtplat.core.eventcenter.Observer;
import com.bj58.zptask.dtplat.core.failstore.AbstractFailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStore;
import com.bj58.zptask.dtplat.core.failstore.FailStoreException;
import com.bj58.zptask.dtplat.core.failstore.FailStoreFactory;
import com.bj58.zptask.dtplat.core.logger.Logger;
import com.bj58.zptask.dtplat.core.logger.LoggerFactory;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.EcTopic;
import com.bj58.zptask.dtplat.util.GenericsUtils;
import com.bj58.zptask.dtplat.util.JSONUtils;

/**
 * 重试定时器 (用来发送 给 客户端的反馈信息等)
 */
public abstract class RetryScheduler<T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(RetryScheduler.class);

    private Class<?> type = GenericsUtils.getSuperClassGenericType(this.getClass());

    // 定时检查是否有 师表的反馈任务信息(给客户端的)
    private ScheduledExecutorService RETRY_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService MASTER_RETRY_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> masterScheduledFuture;
    private ScheduledFuture<?> scheduledFuture;
    private AtomicBoolean selfCheckStart = new AtomicBoolean(false);
    private AtomicBoolean masterCheckStart = new AtomicBoolean(false);
    private FailStore failStore;
    // 名称主要是用来记录日志
    private String name;

    // 批量发送的消息数
    private int batchSize = 5;

    public RetryScheduler(Application application) {
        this(application, application.getConfig().getFailStorePath());
    }

    public RetryScheduler(Application application, String storePath) {
        FailStoreFactory failStoreFactory = InjectorHolder.getInstance(FailStoreFactory.class);
        failStore = failStoreFactory.getFailStore(application.getConfig(), storePath);

        EventSubscriber subscriber = new EventSubscriber(RetryScheduler.class.getSimpleName().concat(application.getConfig().getIdentity()), new Observer() {
            @Override
            public void onObserved(EventInfo eventInfo) {
                Boolean isMaster = (Boolean) eventInfo.getParam("isMaster");
                if (isMaster) {
                    startMasterCheck();
                } else {
                    stopMasterCheck();
                }
            }
        });
        application.getEventCenter().subscribe(subscriber, EcTopic.MASTER_CHANGED);

        if (application.getMasterElector().isCurrentMaster()) {
            startMasterCheck();
        }
    }

    public RetryScheduler(Application application, String storePath, int batchSize) {
        this(application, storePath);
        this.batchSize = batchSize;
    }

    protected RetryScheduler(Application application, int batchSize) {
        this(application);
        this.batchSize = batchSize;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void start() {
        try {
            if (selfCheckStart.compareAndSet(false, true)) {
                // 这个时间后面再去优化
                scheduledFuture = RETRY_EXECUTOR_SERVICE.scheduleWithFixedDelay(new CheckSelfRunner(), 10, 30, TimeUnit.SECONDS);
                LOGGER.info("Start {} RetryScheduler success.", name);
            }
        } catch (Throwable t) {
            LOGGER.error("Start {} RetryScheduler failed.", name, t);
        }
    }

    private void startMasterCheck() {
        try {
            if (masterCheckStart.compareAndSet(false, true)) {
                // 这个时间后面再去优化
                masterScheduledFuture = MASTER_RETRY_EXECUTOR_SERVICE.scheduleWithFixedDelay(new CheckDeadFailStoreRunner(), 30, 60, TimeUnit.SECONDS);
                LOGGER.info("Start {} master RetryScheduler success.", name);
            }
        } catch (Throwable t) {
            LOGGER.error("Start {} master RetryScheduler failed.", name, t);
        }
    }

    private void stopMasterCheck() {
        try {
            if (masterCheckStart.compareAndSet(true, false)) {
                masterScheduledFuture.cancel(true);
                MASTER_RETRY_EXECUTOR_SERVICE.shutdown();
                LOGGER.info("Stop {} master RetryScheduler success.", name);
            }
        } catch (Throwable t) {
            LOGGER.error("Stop {} master RetryScheduler failed.", name, t);
        }
    }

    public void stop() {
        try {
            if (selfCheckStart.compareAndSet(true, false)) {
                scheduledFuture.cancel(true);
                RETRY_EXECUTOR_SERVICE.shutdown();
                LOGGER.info("Stop {} RetryScheduler success.", name);
            }
        } catch (Throwable t) {
            LOGGER.error("Stop {} RetryScheduler failed.", name, t);
        }
    }

    public void destroy() {
        try {
            stop();
            failStore.destroy();
        } catch (FailStoreException e) {
            LOGGER.error("destroy {} RetryScheduler failed.", name, e);
        }
    }

    /**
     * 定时检查 提交失败任务的Runnable
     */
    private class CheckSelfRunner implements Runnable {

        @Override
        public void run() {
            try {
                // 1. 检测 远程连接 是否可用
                if (!isRemotingEnable()) {
                    return;
                }

                List<KVPair<String, T>> kvPairs = null;
                do {
                    try {
                        failStore.open();

                        kvPairs = failStore.fetchTop(batchSize, type);

                        if (CollectionUtils.isEmpty(kvPairs)) {
                            break;
                        }

                        List<T> values = new ArrayList<T>(kvPairs.size());
                        List<String> keys = new ArrayList<String>(kvPairs.size());
                        for (KVPair<String, T> kvPair : kvPairs) {
                            keys.add(kvPair.getKey());
                            values.add(kvPair.getValue());
                        }
                        if (retry(values)) {
                            LOGGER.info("{} RetryScheduler, local files send success, size: {}, {}", name, values.size(), JSONUtils.toJSONString(values));
                            failStore.delete(keys);
                        } else {
                            break;
                        }
                    } finally {
                        failStore.close();
                    }
                } while (CollectionUtils.isNotEmpty(kvPairs));

            } catch (Throwable e) {
                LOGGER.error("Run {} RetryScheduler error.", name, e);
            }
        }
    }

    /**
     * 定时检查 已经down掉的机器的FailStore目录
     */
    private class CheckDeadFailStoreRunner implements Runnable {

        @Override
        public void run() {
            try {
                // 1. 检测 远程连接 是否可用
                if (!isRemotingEnable()) {
                    return;
                }
                List<FailStore> failStores = null;
                if (failStore instanceof AbstractFailStore) {
                    failStores = ((AbstractFailStore) failStore).getDeadFailStores();
                }
                if (CollectionUtils.isNotEmpty(failStores)) {
                    for (FailStore store : failStores) {
                        store.open();

                        while (true) {
                            List<KVPair<String, T>> kvPairs = store.fetchTop(batchSize, type);
                            if (CollectionUtils.isEmpty(kvPairs)) {
                                store.destroy();
                                LOGGER.info("{} RetryScheduler, delete store dir[{}] success.", name, store.getPath());
                                break;
                            }
                            List<T> values = new ArrayList<T>(kvPairs.size());
                            List<String> keys = new ArrayList<String>(kvPairs.size());
                            for (KVPair<String, T> kvPair : kvPairs) {
                                keys.add(kvPair.getKey());
                                values.add(kvPair.getValue());
                            }
                            if (retry(values)) {
                                LOGGER.info("{} RetryScheduler, dead local files send success, size: {}, {}", name, values.size(), JSONUtils.toJSONString(values));
                                store.delete(keys);
                            } else {
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (Exception e) {
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                LOGGER.error("Run {} master RetryScheduler error.", name, e);
            }
        }
    }

    public void inSchedule(String key, T value) {
        try {
            failStore.open();
            try {
                failStore.put(key, value);
                LOGGER.info("{} RetryScheduler, local files save success, {}", name, JSONUtils.toJSONString(value));
            } finally {
                failStore.close();
            }
        } catch (FailStoreException e) {
            LOGGER.error("{} RetryScheduler in schedule error. ", name, e);
        }
    }

    /**
     * 远程连接是否可用
     *
     * @return
     */
    protected abstract boolean isRemotingEnable();

    /**
     * 重试
     *
     * @param list
     * @return
     */
    protected abstract boolean retry(List<T> list);

}
