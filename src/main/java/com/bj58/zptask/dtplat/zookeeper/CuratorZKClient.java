package com.bj58.zptask.dtplat.zookeeper;

import java.io.Serializable;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.registry.CuratorPlateRegistry;
import com.bj58.zptask.dtplat.registry.event.NodeAddEvent;
import com.bj58.zptask.dtplat.registry.event.NodeRemoveEvent;
import com.bj58.zptask.dtplat.util.SerializeUtil;
import com.google.inject.Singleton;

/**
 * ZK的CURATOR客户端实现
 * 这是一个公共类  主要为 registry 分布式的锁 提供服务
 * 
 * 
 * @author WuTong
 * @version 1.0
 * @date  2015年9月9日 下午3:02:06
 * @see 
 * @since
 */
@Singleton
public class CuratorZKClient extends AbstractZKClient<CuratorWatcher> {
    private static final Logger logger = Logger.getLogger(CuratorZKClient.class);

    private final CuratorFramework client;
    private final RetryPolicy policy = new RetryNTimes(10, 1000);

    public CuratorZKClient() {
        try {
            String registryAddress = InjectorHolder.getInstance(Config.class).getRegistryAddress();
            logger.info(registryAddress);
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().namespace("dtplat").connectString(NodePathHelper.getRealRegistryAddress(registryAddress)).retryPolicy(policy).connectionTimeoutMs(5000);

            client = builder.build();

            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
                public void stateChanged(CuratorFramework client, ConnectionState state) {
                    if (state == ConnectionState.LOST) {
                        CuratorZKClient.this.stateChanged(StateListener.DISCONNECTED);
                    } else if (state == ConnectionState.CONNECTED) {
                        CuratorZKClient.this.stateChanged(StateListener.CONNECTED);
                    } else if (state == ConnectionState.RECONNECTED) {
                        CuratorZKClient.this.stateChanged(StateListener.RECONNECTED);
                    }
                }
            });
            client.start();
        } catch (Exception e) {
            logger.error(InjectorHolder.getInstance(Config.class).getRegistryAddress(), e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public CuratorFramework getClient() {
        return client;
    }

    @Override
    protected String createPersistent(String path, boolean sequential) {
        try {
            if (sequential) {
                return client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path);
            } else {
                return client.create().withMode(CreateMode.PERSISTENT).forPath(path);
            }
        } catch (KeeperException.NodeExistsException e) {
            return path;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected String createPersistent(String path, Object data, boolean sequential) {
        try {
            if (sequential) {
                byte[] zkDataBytes;
                if (data instanceof Serializable) {
                    zkDataBytes = SerializeUtil.jdkSerialize(data);
                } else {
                    zkDataBytes = (byte[]) data;
                }
                return client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, zkDataBytes);
            } else {
                return client.create().withMode(CreateMode.PERSISTENT).forPath(path);
            }
        } catch (KeeperException.NodeExistsException e) {
            return path;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected String createEphemeral(String path, boolean sequential) {
        try {
            if (sequential) {
                return client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
            } else {
                return client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
            }
        } catch (KeeperException.NodeExistsException e) {
            return path;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected String createEphemeral(String path, Object data, boolean sequential) {
        try {
            if (sequential) {
                byte[] zkDataBytes;
                if (data instanceof Serializable) {
                    zkDataBytes = SerializeUtil.jdkSerialize(data);
                } else {
                    zkDataBytes = (byte[]) data;
                }
                return client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, zkDataBytes);
            } else {
                return client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
            }
        } catch (KeeperException.NodeExistsException e) {
            return path;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected CuratorWatcher createTargetChildListener(String path, ChildListener listener) {
        return new CuratorWatcherImpl(listener);
    }

    @Override
    protected List<String> addTargetChildListener(String path, CuratorWatcher watcher) {
        try {
            return client.getChildren().usingWatcher(watcher).forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void removeTargetChildListener(String path, CuratorWatcher listener) {
        ((CuratorWatcherImpl) listener).unwatch();
    }

    @Override
    public boolean delete(String path) {
        try {
            client.delete().forPath(path);
            return true;
        } catch (KeeperException.NoNodeException e) {
            return true;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean exists(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public <T> T getData(String path) {
        try {
            return SerializeUtil.jdkDeserialize(client.getData().forPath(path));
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setData(String path, Object data) {
        byte[] zkDataBytes;
        try {
            if (data instanceof Serializable) {
                zkDataBytes = SerializeUtil.jdkSerialize(data);
            } else {
                zkDataBytes = (byte[]) data;
            }

            client.setData().forPath(path, zkDataBytes);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 
     * @param path
     * @param cacheData
     * @return
     * @throws Exception
     */

    @Override
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    @Override
    protected void doClose() {
        CloseableUtils.closeQuietly(client);
        //        client.close();
    }

    //全是注册订阅模式
    private class CuratorWatcherImpl implements CuratorWatcher {

        private volatile ChildListener listener;

        public CuratorWatcherImpl(ChildListener listener) {
            this.listener = listener;
        }

        public void unwatch() {
            this.listener = null;
        }

        public void process(WatchedEvent event) throws Exception {
            if (listener != null) {
                //重复注册啊
                listener.childChanged(event.getPath(), client.getChildren().usingWatcher(this).forPath(event.getPath()));
            }
        }
    }

}
