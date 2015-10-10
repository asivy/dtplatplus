package com.bj58.zptask.dtplat.commons;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
import com.bj58.zhaopin.feature.contract.INodeGroupService;
import com.bj58.zhaopin.feature.contract.ITaskDefineService;
import com.bj58.zhaopin.feature.contract.ITaskExecutableService;
import com.bj58.zhaopin.feature.contract.ITaskExecutingService;
import com.bj58.zhaopin.feature.contract.ITaskLogService;
import com.bj58.zhaopin.feature.entity.NodeGroupBean;
import com.bj58.zhaopin.feature.entity.TaskDefineBean;
import com.bj58.zhaopin.feature.entity.TaskExecutableBean;
import com.bj58.zhaopin.feature.entity.TaskExecutingBean;
import com.bj58.zhaopin.feature.entity.TaskLogBean;
import com.bj58.zptask.dtplat.zookeeper.DistributeLock;
import com.google.inject.Singleton;

@Singleton
public class DTaskProvider {
    private static final Logger logger = Logger.getLogger(DTaskProvider.class);

    private static final String ITaskLogServiceStr = "tcp://feature/TaskLogServiceImpl";

    private static final String INodeGroupServiceStr = "tcp://feature/NodeGroupServiceImpl";

    private static final String ITaskDefineServiceStr = "tcp://feature/TaskDefineServiceImpl";

    private static final String ITaskExecutableServiceStr = "tcp://feature/TaskExecutableServiceImpl";

    //    private static final String ITaskFeedbackServiceStr = "tcp://feature/TaskFeedbackServiceImpl";

    private static final String ITaskExecutingServiceStr = "tcp://feature/TaskExecutingServiceImpl";

    public ITaskLogService getITaskLogService() {
        ITaskLogService tasklogservice = null;
        try {
            tasklogservice = ProxyFactory.create(ITaskLogService.class, ITaskLogServiceStr);
        } catch (Exception e) {
            logger.info("create ITaskLogService Error.");
        }
        return tasklogservice;
    }

    public INodeGroupService getINodeGroupService() {
        INodeGroupService nodegroupservice = null;
        try {
            nodegroupservice = ProxyFactory.create(INodeGroupService.class, INodeGroupServiceStr);
        } catch (Exception e) {
            logger.info("create INodeGroupService Error.");
        }
        return nodegroupservice;
    }

    public ITaskExecutableService getITaskExecutetableService() {
        ITaskExecutableService taskexecutable = null;
        try {
            taskexecutable = ProxyFactory.create(ITaskExecutableService.class, ITaskExecutableServiceStr);
        } catch (Exception e) {
            logger.info("create ITaskExecutableService Error.");
        }
        return taskexecutable;
    }

    public ITaskExecutingService getIExecutingService() {
        ITaskExecutingService taskExecuting = null;
        try {
            taskExecuting = ProxyFactory.create(ITaskExecutingService.class, ITaskExecutingServiceStr);
        } catch (Exception e) {
            logger.info("create IExecutingService Error.");
        }
        return taskExecuting;
    }

    public ITaskDefineService getITaskDefineService() {
        ITaskDefineService featureservice = null;
        try {
            featureservice = ProxyFactory.create(ITaskDefineService.class, ITaskDefineServiceStr);
        } catch (Exception e) {
            logger.info("create ITaskDefineService Error.");
        }
        return featureservice;
    }

    /**---------------------------------------------------------------*/

    // TaskLog服务：insert
    public long addTaskLog(TaskLogBean bean) {
        try {
            return getITaskLogService().insert(bean);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }

    /**
     * 先判断此分组是否存在  
     * 如果不存在 则添加
     * @param nodeType
     * @param name
     * @return
     */
    public long addNodeGroup(String nodeType, String name) {
        try {
            NodeGroupBean nodegroup = getINodeGroupService().loadByTypeName(nodeType, name);
            if (nodegroup == null) {
                nodegroup = new NodeGroupBean();
                nodegroup.setName(name);
                nodegroup.setNodeType(nodeType);
                nodegroup.setCreateDate(new Date());
                return getINodeGroupService().insert(nodegroup);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }

    // NodeGroup服务：delete
    public long deleteNodeGroup(String nodetype, String name) {
        try {
            return getINodeGroupService().delete(nodetype, name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }

    // NodeGroup服务：loadNodeGroupByType
    public List<NodeGroupBean> loadNodeGroupByType(String nodetype) {
        try {
            return getINodeGroupService().loadByType(nodetype);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // TaskDefine服务： insert 
    public long addTaskDefine(TaskDefineBean taskDefineBean) {
        try {
            return getITaskDefineService().insert(taskDefineBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;

    }

    /**
     * 获得一个任务的定义
     * @param jobID
     * @return
     */
    public TaskDefineBean loadTaskDefineByJobID(long jobID) {
        try {
            return getITaskDefineService().loadByJobID(jobID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public int deleteTaskDefineByJobID(long jobID) {
        try {
            return getITaskDefineService().deleteByJobID(jobID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**-------------------TaskExecutable业务 ---------------------------------*/

    public long addExecutableTask(TaskExecutableBean taskexecutable) {
        try {
            //设置下创建和记录时间
            taskexecutable.setCreateDate(new Date());
            taskexecutable.setModifyDate(new Date());
            System.out.println("add executable :" + JSON.toJSONString(taskexecutable));
            return getITaskExecutetableService().insert(taskexecutable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }

    /**
     * 功能：获取一个待执行的任务  因为是分布式情况  防一个任务被执行多次
     * 
     * 原业务模拟了一个数据库表CAS操作:
     * 1 获取信号量  不能超过最大值限制
     * 2 查询未执行的  触发时间 小于当前时间的 
     * 3 将任务的状态置为正在执行
     * 4 如果任务的状态已经被设置过了  则重复上面的步骤   感觉此处可能会引起死锁或死循环问题
     * 5 如果此任务没有被设置过  则返回此任务
     * 
     * 可以用zk的分布式可重入锁对其进行改造
     * 
     * @param taskTrackerNodeGroup  task组名
     * @param taskTrackerIdentity   执行此任务的具体task
     * @return
     * Note:原数据是按组分表存的  新接口中把组都去掉了
     * ToSee MysqlExecutableJobQueue
     */
    public TaskExecutableBean takeExecutaleTask(final String taskGroup, final String taskIdentity) {
        InterProcessMutex lock = null;
        try {
            lock = DistributeLock.getInterProcessMutex(taskGroup);
            lock.acquire(1000, TimeUnit.MILLISECONDS);

            TaskExecutableBean taskExecutable = getITaskExecutetableService().loadOneByGroupRunningTrigger(taskGroup, false, new Date());
            if (taskExecutable == null) {
                return null;
            }
            
            //更新此任务的状态
            getITaskExecutetableService().updateByJobIDRunning(taskExecutable.getJobId(), false, taskIdentity, new Date());
            //            if (infectCnt < 1) {
            //                //takeExecutaleTask()  
            //            }
            taskExecutable.setRunning(true);
            taskExecutable.setTaskIdentity(taskIdentity);
            taskExecutable.setModifyDate(new Date());
            System.out.println("result=" + JSON.toJSONString(taskExecutable));
            return taskExecutable;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DistributeLock.release(lock);
        }
        return null;
    }

    /**
     * 重置一个任务的状态    
     * 对僵尸任务重置状态
     * @param jobId
     */
    public void resetExecutableTask(long jobId, String taskGroup) {
        try {
            getITaskExecutetableService().updateByJobID(jobId, taskGroup, new Date(), false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<TaskExecutableBean> loadDeadTask(String nodeGroup, Date modifyDate) {
        try {
            return getITaskExecutetableService().loadByModifyRunning(true, modifyDate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除一个任务
     * 执行完成后触发
     * @param jobId
     * @param taskTrackerNodeGroup
     */
    public void deleteExecutableTask(long jobId, String taskGroup) {
        try {
            getITaskExecutetableService().deleteByJobID(jobId, taskGroup);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**-------------------TaskExecuting业务 ---------------------------------*/
    public long addExecutingTask(TaskExecutingBean taskExecuting) {
        try {
            Date now = new Date();
            taskExecuting.setCreateDate(now);
            taskExecuting.setModifyDate(now);
            System.out.println("add executing: " + JSON.toJSONString(taskExecuting));
            return getIExecutingService().insert(taskExecuting);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }

    public void deleteExecutingTaskByJobID(long jobID) {
        try {
            System.out.println("delete executing jobid = " + jobID);
            getIExecutingService().deleteByJobID(jobID);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TaskExecutingBean loadTaskExecutingTaskByJobID(long jobID) {
        try {
            return getIExecutingService().loadByJobID(jobID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<TaskExecutingBean> loadTaskExecutingTasksBytaskTrackerIdentity(String taskIdentity) {
        try {
            return getIExecutingService().loadByTaskTracker(taskIdentity);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<TaskExecutingBean> loadDeadTaskExecutingTasks(Date createDate) {
        try {
            return getIExecutingService().loadByCreateDate(createDate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
