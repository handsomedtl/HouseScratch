package com.dtl.thread;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.dtl.OperatorService;

public class ConsumerCallableThread implements Callable<Boolean> {
	private static Logger logger = Logger.getLogger(ConsumerCallableThread.class);  
	
	private OperatorService operatorService;
	
	public ConsumerCallableThread(OperatorService operatorService) {
		
		this.operatorService = operatorService;
	}	

	@Override
	public Boolean call() throws Exception {
		logger.info("━━━━消费者线程启动, ThreadId = " + Thread.currentThread().getId() + "━━━━");
		operatorService.saveToDB();
		
		logger.info(String.format("━━━━━━━━消费者线程 ThreadId = %d 退出━━━━━━━━", Thread.currentThread().getId()));
		
		return true;
	}
}
