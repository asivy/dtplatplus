package com.bj58.zptask.dtplat.core.registry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.bj58.zptask.dtplat.core.cluster.Config;
import com.bj58.zptask.dtplat.core.cluster.Node;
import com.bj58.zptask.dtplat.util.CollectionUtils;
import com.bj58.zptask.dtplat.util.ConcurrentHashSet;

/**
 * 注册中心的管理类
 * 
 * 
 * 
 * @author WuTong 
 * @version 1.0
 * @date  2015年9月21日 下午2:07:45
 * @see 
 * @since
 */
public abstract class AbstractRegistry implements Registry {

    protected final static Logger logger = Logger.getLogger(AbstractRegistry.class);

    private final Set<Node> registered = new ConcurrentHashSet<Node>();
    private final ConcurrentMap<Node, Set<NotifyListener>> subscribed = new ConcurrentHashMap<Node, Set<NotifyListener>>();

    protected Config config;
    private Node node;

    public AbstractRegistry(Config config) {
        this.config = config;
    }

    @Override
    public void register(Node node) {
        if (node == null) {
            throw new IllegalArgumentException("register node == null");
        }
        registered.add(node);
    }

    @Override
    public void unregister(Node node) {
        if (node == null) {
            throw new IllegalArgumentException("unregister node == null");
        }
        registered.remove(node);
    }

    @Override
    public void subscribe(Node node, NotifyListener listener) {
        if (node == null) {
            throw new IllegalArgumentException("subscribe node == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        Set<NotifyListener> listeners = subscribed.get(node);
        if (listeners == null) {
            subscribed.putIfAbsent(node, new ConcurrentHashSet<NotifyListener>());
            listeners = subscribed.get(node);
        }
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(Node node, NotifyListener listener) {
        if (node == null) {
            throw new IllegalArgumentException("unsubscribe node == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        Set<NotifyListener> listeners = subscribed.get(node);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    /**
     * 事件通知   这个地方没必要通知啊
     * @param event
     * @param nodes
     * @param listener
     */
    protected void notify(NotifyEvent event, List<Node> nodes, NotifyListener listener) {
        if (event == null) {
            throw new IllegalArgumentException("notify event == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if (CollectionUtils.isEmpty(nodes)) {
            logger.warn("Ignore empty notify nodes for subscribe node " + getNode());
            return;
        }

        listener.notify(event, nodes);
    }
    
    @Override
    public void destroy() {
        Set<Node> destroyRegistered = new HashSet<Node>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (Node node : new HashSet<Node>(getRegistered())) {
                try {
                    unregister(node);
                } catch (Throwable t) {
                    logger.warn("Failed to unregister node " + node + " to registry " + getNode() + " on destroy, cause: " + t.getMessage(), t);
                }
            }
        }
        
        Map<Node, Set<NotifyListener>> destroySubscribed = new HashMap<Node, Set<NotifyListener>>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<Node, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                Node node = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(node, listener);
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe node " + node + " to registry " + getNode() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    protected Set<Node> getRegistered() {
        return registered;
    }

    protected ConcurrentMap<Node, Set<NotifyListener>> getSubscribed() {
        return subscribed;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    /**
     * 恢复
     *
     * @throws Exception
     */
    protected void recover() throws Exception {
        // register
        Set<Node> recoverRegistered = new HashSet<Node>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            for (Node node : recoverRegistered) {
                register(node);
            }
        }
        // subscribe
        Map<Node, Set<NotifyListener>> recoverSubscribed = new HashMap<Node, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            for (Map.Entry<Node, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                Node node = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    subscribe(node, listener);
                }
            }
        }
    }

}
