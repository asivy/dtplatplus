注册中心这一块需要重写:  
1 完全理解这一块的逻辑；
2 去掉丑陋的 listener 机制; 
3 zk的管理与注册 都在这里呢
4 对zk树进行修改

分步骤讨论：
1 +job 
2 +job
3 +task  job可以受到task的变化    但task怎么知道已有的两个job呢
4 +task  job可以接通受到task   同样task无法知道job
5 +job   

所有节点都只能监听到之后的变化    对之前的变化节点无法感知到
