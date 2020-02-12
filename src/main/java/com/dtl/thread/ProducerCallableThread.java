package com.dtl.thread;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.openqa.selenium.chrome.ChromeDriver;

import com.dtl.OperatorService;
import com.dtl.item.PCData;

public class ProducerCallableThread implements Callable<List<PCData>> {
	private static Logger logger = Logger.getLogger(ProducerCallableThread.class);
	private List<PCData> houseList;
	private OperatorService operatorService;
	private ChromeDriver driver;
	private int currIndex;

	public ProducerCallableThread(final List<PCData> list, int startIndex, int endIndex,
			OperatorService operatorService, int index) {
		if (endIndex < startIndex)
			throw new IllegalArgumentException("List 开始下标大于结束下标");

		houseList = list.subList(startIndex, endIndex);
		this.operatorService = operatorService;

		currIndex = index;
	}


	@Override
	public List<PCData> call() throws Exception {
		logger.info("━━━━生产者线程启动, ThreadId = " + Thread.currentThread().getId() + "  本线程处理房子数：" + houseList.size()
				+ "━━━━");

		driver = this.operatorService.createWebDriver(currIndex);

		List<PCData> errorList = this.operatorService.scanHouses(houseList, driver);

		logger.info(String.format("━━━━━━━━生产者线程 ThreadId = %d 退出━━━━━━━━\n", Thread.currentThread().getId()));
		
		return errorList;
	}

}
