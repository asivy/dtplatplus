//package com.bj58.zptask.dtplat.test.feature;
//
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
//import org.apache.log4j.Logger;
//import org.apache.zookeeper.Op.Delete;
//import org.junit.Test;
//
//import com.alibaba.fastjson.JSON;
//import com.bj58.spat.scf.client.SCFInit;
//import com.bj58.spat.scf.client.proxy.builder.ProxyFactory;
//import com.bj58.spat.scf.server.contract.entity.Out;
//import com.bj58.zhaopin.feature.contract.ITaskFeedbackService;
//import com.bj58.zhaopin.feature.contract.ITaskLogService;
//import com.bj58.zhaopin.feature.entity.TaskFeedbackBean;
//import com.bj58.zhaopin.feature.entity.TaskLogBean;
//import com.bj58.zhaopin.feature.model.TrainModelBootStrap;
//
//public class TaskFeedbackTest {
//	
//	private static final Logger logger = Logger.getLogger(TrainModelBootStrap.class);
//	
//	static ITaskFeedbackService service;
//    static {
//        try {
//            SCFInit.init("E:/opt/wf/com.bj58.zhaopin.web.foresee/scf.config");
//            service = ProxyFactory.create(ITaskFeedbackService.class, "tcp://feature/TaskFeedbackServiceImpl");
//        } catch (Exception e) {
//        	logger.error("SCF init Error");
//        	System.out.println("SCF init Error");
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void insertTest() {
//        try {
//            for (int i = 0; i < 10; i++) {
//            	TaskFeedbackBean entity = new TaskFeedbackBean();
//            	
//            	entity.setCreateDate(new Date());
//            	entity.setNodeGroup("" + (i + 1));
//            	entity.setResult("This is the " + (i + 1) + "th FeedbackTest");
//                long id = service.insert(entity);
//                System.out.println("id=" + id);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//    
//    @Test
//    public void insertBatchTest() {
//    	try {
//    		List<TaskFeedbackBean> list = new ArrayList<TaskFeedbackBean>();
//    		
//    		TaskFeedbackBean entityNode1 = new TaskFeedbackBean();
//    		entityNode1.setCreateDate(new Date());
//        	entityNode1.setNodeGroup("Group 1");
//        	entityNode1.setResult("This is the Node1 FeedbackTest");
//        	
//    		TaskFeedbackBean entityNode2 = new TaskFeedbackBean();
//    		entityNode2.setCreateDate(new Date());
//        	entityNode2.setNodeGroup("Group 2");
//        	entityNode2.setResult("This is the Node2 FeedbackTest");
//        	
//    		TaskFeedbackBean entityNode3 = new TaskFeedbackBean();
//    		entityNode3.setCreateDate(new Date());
//        	entityNode3.setNodeGroup("Group 3");
//        	entityNode3.setResult("This is the Node3 FeedbackTest");
//        	
//        	list.add(entityNode1);
//        	list.add(entityNode2);
//        	list.add(entityNode3);
//        	
//        	List<Long> ids = service.insertBatch(list);
//        	for(Long id : ids) {
//        		System.out.println("id = " + id);
//        	}
//    	} catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void countAllTest() {
//    	try {
//	        int sum = service.countAll();
//	        System.out.println("sum = " + sum);
//        } catch (Exception e) {
//	        e.printStackTrace();
//        }
//    }
//    
//    @Test
//    public void countByGroupTest() {
//    	try {
//    		String group = "3";
//    		long sum = service.countByGroup(group);
//    		System.out.println("sum = " + sum);
//    	} catch (Exception e) {
//    		e.printStackTrace();
//    	}
//    }
//    
//    @Test
//    public void deleteByGroupTest() {
//    	String group = "3";
//    	long id = 78l;
//    	try {
//	        service.deleteByGroupJob(group, id);
//        } catch (Exception e) {
//	        e.printStackTrace();
//        }
//    }
//    
//    @Test
//    public void fetchTopByGroup() {
//    	try {
//    		String group = "5";
//    		int top = 3;
//    		List<TaskFeedbackBean> list = new ArrayList<TaskFeedbackBean>();
//    		list = service.fetchTopByGroup(group, top);
//    		for(TaskFeedbackBean one : list) {
//    			System.out.println("id = " + one.getID() + " / group = " + one.getNodeGroup());
//    		}
//    	} catch(Exception e) {
//    		e.printStackTrace();
//    	}
//    }
//    
////    @Test
////    public void query() {
////        try {
////            List<TaskFeedbackBean> total = new ArrayList<TaskFeedbackBean>();
////            List<TaskFeedbackBean> list = service.loadByPage(5, 3, new Date(), 30, 1, total);
////            if (list != null) {
////                for (TaskLogBean bean : list) {
////                    System.out.println(JSON.toJSONString(bean));
////                }
////            }
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
////    }
//}
