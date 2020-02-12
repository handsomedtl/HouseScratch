package com.dtl;

import org.apache.log4j.Logger;

public class ConsumerThread implements Runnable {
	private static Logger logger = Logger.getLogger(ConsumerThread.class);  
	
	private OperatorService operatorService;

	public ConsumerThread(OperatorService operatorService) {		
		this.operatorService = operatorService;
	}	

	@Override
	public void run() {
		logger.info("━━━━消费者线程启动, ThreadId = " + Thread.currentThread().getId() + "━━━━");
		operatorService.saveToDB();
		
		logger.info(String.format("━━━━━━━━消费者线程 ThreadId = %d 退出━━━━━━━━", Thread.currentThread().getId()));
	}

}
