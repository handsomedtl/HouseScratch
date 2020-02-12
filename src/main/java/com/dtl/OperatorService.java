package com.dtl;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeDriverService;
import org.openqa.selenium.chrome.ChromeOptions;

import com.dtl.item.PCData;

import io.restassured.RestAssured;

public class OperatorService {
	private static Logger logger = Logger.getLogger(OperatorService.class);
	
	private static String PNG_TYPE_PUZZLE = "puzzle";
	
	private String [] userAgents = {"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36",
			"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
			"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
			"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
			"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
			"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
			"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE",
			"Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv 11.0) like Gecko",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586",
			"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36 Core/1.63.5680.400 QQBrowser/10.2.1852.400"};

	
	public static ReentrantLock lock = new ReentrantLock();
    public static Condition empty = lock.newCondition();
    public static Condition full = lock.newCondition();
	
	public static AtomicInteger offLineCount;	//当前下架的房子数
	public static AtomicInteger deleteCount;	//当前删除的房子数
	public static AtomicInteger reOnLineCount;	//重新上架的房子数
	
	private static AtomicInteger count;	//当前已处理的房子数
	private static AtomicLong lastTime;	//上次统计时间
	private static AtomicInteger lastCount;
		
	private CountDownLatch mDoneSignal;
	private List<PCData> queue;
	
	private ChromeDriverService service;
	private Connection conn;
	
	private int houseTotal;
	
	
	
	public OperatorService() {
		
	}

	public OperatorService(ChromeDriverService service,CountDownLatch mDoneSignal,Connection conn,int houseTotal) {
		this.mDoneSignal = mDoneSignal;
		this.conn = conn;
		this.service = service;
		
		if(null == offLineCount)
			offLineCount = new AtomicInteger();
		if(null == deleteCount)
			deleteCount = new AtomicInteger();
		if(null == reOnLineCount)
			reOnLineCount = new AtomicInteger();
		if(null == count)
			count = new AtomicInteger();		
		if(null == lastTime)
			lastTime = new AtomicLong(System.currentTimeMillis());
		if(null == lastCount)
			lastCount = new AtomicInteger();
		
		this.houseTotal = houseTotal;
		queue = new ArrayList<PCData>();
	}
	
	public void saveToDB() {
		try {
			while (this.houseTotal >= count.get()) {
				if (Thread.currentThread().isInterrupted())
					break;
				
				PCData data = null;
				HouseScratch.lock.lock();
				
				if (queue.isEmpty()) {
					if(count.get() > 0) {
						long costTime = System.currentTimeMillis() - lastTime.get();				
						
						float ratio = 1000.0f * (count.get() - lastCount.get())/costTime;
						
						if (0.0 < ratio) {
							float remainTime = (this.houseTotal - count.get()) / (ratio * 60);
							logger.info(String.format("共有%d套待处理，当前已经扫描%d(%.2f %%)，还需%.1f分钟", this.houseTotal,
									count.get(), (100.0f * count.get() / this.houseTotal), remainTime));
							
							if("true".equalsIgnoreCase(HouseScratch.resource.getString("shutdown"))) {
								try {
									//关闭计算机
									if(count.get() > Integer.valueOf(HouseScratch.resource.getString("queue.size")))
										Runtime.getRuntime().exec("shutdown -a");	//取消上次记时
									
									Runtime.getRuntime().exec("shutdown /s /t " + (int)(remainTime * 60 + 120));									
								} catch (Exception e) {
									logger.error(String.format("关机调用失败 (ThreadId = %d)", Thread.currentThread().getId()));
								}
							}
						}
						lastTime.lazySet(System.currentTimeMillis());
						lastCount.set(count.get());
					}
					if(count.get() >= this.houseTotal)
						break;
					//------------------------------------					
					HouseScratch.full.signalAll();
					logger.debug("消费者 threadId =" + Thread.currentThread().getId() + "  睡眠");
					HouseScratch.empty.await();
				}
				if(!queue.isEmpty()) {
					data = queue.remove(0);
					logger.debug("消费者 threadId =" + Thread.currentThread().getId() + "  取一个元素");					
					count.incrementAndGet();
				}
				
				HouseScratch.lock.unlock();
				if(null != data && data.isChanged()) {				
					if(HouseScratch.STATUS_OFFLINE == data.getStatus()) {			
						updateDB(data.getHouseId(), HouseScratch.STATUS_OFFLINE);					
					}
					else if(HouseScratch.STATUS_DEL == data.getStatus()) {
						updateDB(data.getHouseId(), HouseScratch.STATUS_DEL);					
					}
					else if(HouseScratch.STATUS_ONLINE == data.getStatus()) {
						updateDB(data.getHouseId(), HouseScratch.STATUS_ONLINE);					
					}
				}
			}			
		} 
		catch (InterruptedException e) {
			logger.error(String.format("\t消费者线程 ThreadId = %d 被中止", Thread.currentThread().getId()));
		}
		catch (Exception e) {
			logger.error(String.format("\t消费者线程 ThreadId = %d 被中止", Thread.currentThread().getId()), e);
		}
		finally{
			mDoneSignal.countDown();
		}
	}
	
	public ChromeDriver createWebDriver(Integer index) {
		ChromeDriver driver = new ChromeDriver(service, initChromeOpts(index));
		
		return driver;
	}
	
	
	public List<PCData> scanHouses(List<PCData> houseList,ChromeDriver driver) {
		List<PCData> errorList = new ArrayList<>();
		
		try {
			Iterator<PCData> iter = houseList.iterator();
			
			int delayCount = 0; //设置延时个数
			//设置标签查找不到的等待时间
			//driver.manage().timeouts().implicitlyWait(6, TimeUnit.SECONDS);
			
			final int delay = Integer.valueOf(HouseScratch.resource.getString("delaytime"));
			if(delay > 0)
				driver.manage().timeouts().pageLoadTimeout(delay, TimeUnit.SECONDS);
			
			while (iter.hasNext() && !Thread.interrupted()) {
				PCData item = iter.next();
				long startTime = System.currentTimeMillis();
				int status;
				try {
					status = checkHouseOnLineInMobile(item,driver);
					long gap = System.currentTimeMillis() - startTime;
					
					if(gap > delay*1000) {
						logger.info("ThreadId = " + Thread.currentThread().getId() + "  " + item 
								+ " 处理时间超过 " + delay + " 秒--" + gap + " ms (" + (++delayCount)+ "个)");
					}
				} 
				catch (Exception e) {
					logger.error(item.toString() + " 处理失败，currentThread = " + Thread.currentThread().getId(),e);
					errorList.add(item);
					continue;
				}
				
				PCData data = new PCData();
				if(item.getStatus() != status) {
					data.setChanged(true);
					if(status == HouseScratch.STATUS_OFFLINE) {
						String url = "https://tj.lianjia.com/ershoufang/"+ item + ".html";
						logger.info(url + " 已下架");
						offLineCount.incrementAndGet();					
					}
					else if(status == HouseScratch.STATUS_DEL) {
						String url = "https://tj.lianjia.com/ershoufang/"+ item + ".html";
						logger.info(url + " 已删除");
						deleteCount.incrementAndGet();									
					}
					else if(status == HouseScratch.STATUS_ONLINE) {
						String url = "https://tj.lianjia.com/ershoufang/"+ item + ".html";
						logger.info(url + " 重新上线");
						reOnLineCount.incrementAndGet();	
					}
					else {
						String url = "https://tj.lianjia.com/ershoufang/"+ item + ".html";
						logger.info(url + " 未知状态  status=" + status + " getStatus()=" + item.getStatus());
						errorList.add(item);
						
						if(0.1 < ((double)delayCount)/houseList.size()) {
							logger.error(String.format("延时个数超过本次处理个数（%d）的10%%（%d）,本线程（ThreadId = %d）退出.",houseList.size(), delayCount,Thread.currentThread().getId()));
							if(houseList.indexOf(item)+1 < houseList.size())
								errorList.addAll(houseList.subList(houseList.indexOf(item)+1,houseList.size()));
							
							break;
						}
						
						data = null;
						continue;
					}
				}
				else {
					data.setChanged(false);
				}
				
				data.setHouseId(item.getHouseId());
				data.setStatus(status);
				
				HouseScratch.lock.lock();
				
                if(queue.size() >= Integer.valueOf(HouseScratch.resource.getString("queue.size"))){
                	HouseScratch.empty.signalAll();
                	HouseScratch.full.await();
                }
                
                queue.add(data);
                logger.debug("生产者 threadId =" + Thread.currentThread().getId() + "  写入一个元素");
                HouseScratch.lock.unlock();
                
                if(0.1 < ((double)delayCount)/houseList.size()) {
					logger.error(String.format("☆延时个数超过本次处理个数（%d）的10%%（%d）,本线程（ThreadId = %d）退出.",houseList.size(), delayCount,Thread.currentThread().getId()));
					if(houseList.indexOf(item)+1 < houseList.size())
						errorList.addAll(houseList.subList(houseList.indexOf(item)+1,houseList.size()));
					
					break;
				}
			}
			
			closeBrowser(driver);
			
			if(HouseScratch.lock.tryLock()) {
				HouseScratch.empty.signalAll();
				HouseScratch.lock.unlock();				
			}
        	
			mDoneSignal.countDown();
		} 
		catch (InterruptedException e) {
			logger.error(String.format("\t生产者线程 ThreadId = %d 被中止", Thread.currentThread().getId()) , e);
		}
		catch (Exception e) {
			logger.error(String.format("\t生产者线程 ThreadId = %d 被中止", Thread.currentThread().getId()) , e);
		}
		return errorList;
	}
	
	/**
	 * 保存统计信息
	 */
	public void saveStatistics() {	
		try {
			CallableStatement proc = conn.prepareCall("{ call countStatics(?) }");
			proc.setDate(1, new Date(System.currentTimeMillis()));
			proc.execute();
			logger.info("保存统计信息成功!");
		} 
		catch (SQLException e) {
			logger.error("执行存储过程失败",e);
		}
		catch(Exception e){
			logger.error("执行存储过程失败",e);
		}
	}
	
	private void closeBrowser(ChromeDriver driver) {
		if(null != driver) {
			driver.close();
		}
	}
	
	

	private ChromeOptions initChromeOpts(int index) {
        ChromeOptions chromeOptions = new ChromeOptions();       
        HashMap<String, Object> chromePrefs = new HashMap<String, Object>();
        //禁止弹窗
        chromePrefs.put("profile.default_content_settings.popups", 0);
        //下载地址
        //chromePrefs.put("download.default_directory", "C://xx//");
        //禁止图片加载
        chromePrefs.put("profile.managed_default_content_settings.images", 2);
        
        //动态设置浏览器标识
        index = index % 10;
        //chromePrefs.put("profile.general_useragent_override", userAgents[index]);
		chromeOptions.addArguments("user-agent=" + userAgents[index]);
        
        
        
        chromeOptions.setExperimentalOption("prefs",chromePrefs);
        
        /***********************************以下设置启动参数******************************************/
        //消除安全校验
        chromeOptions.addArguments("--allow-running-insecure-content");
        //启动最大化，防止失去焦点
        //chromeOptions.addArguments("--start-maximized");
        //关闭gpu图片渲染
        chromeOptions.addArguments("--disable-gpu");
		chromeOptions.addArguments("--headless"); 
        
        
        return chromeOptions;
    }
	
	private void updateDB(String houseNo,int status) {
		if(null == conn)
			throw new IllegalArgumentException("数据库没有连接...");
		
		try {
			PreparedStatement pstmt1 = null, pstmt2 = null;
			conn.setAutoCommit(false);	//开启事务
			
			if(HouseScratch.STATUS_OFFLINE == status ||  HouseScratch.STATUS_DEL == status) {
				pstmt1 = conn.prepareStatement(HouseScratch.resource.getString("OffLine_Sql"));
				pstmt1.setInt(1, status);
				pstmt1.setDate(2, new Date(System.currentTimeMillis()));
				pstmt1.setString(3, houseNo);
				pstmt1.executeUpdate();
				
				pstmt2 = conn.prepareStatement(HouseScratch.resource.getString("InsertEvent_Sql"));
				pstmt2.setInt(1, status);
				pstmt2.setString(2, houseNo);
				pstmt2.setDate(3, new Date(System.currentTimeMillis()));
				pstmt2.executeUpdate();
				
			}
			else if (HouseScratch.STATUS_ONLINE == status) {
				//重新上线
				pstmt1 = conn.prepareStatement(HouseScratch.resource.getString("OnLine_Sql"));
				pstmt1.setInt(1, status);
				pstmt1.setString(2, houseNo);
				pstmt1.executeUpdate();
				
				pstmt2 = conn.prepareStatement(HouseScratch.resource.getString("InsertEvent_Sql"));
				pstmt2.setInt(1, HouseScratch.STATUS_REONLINE);
				pstmt2.setString(2, houseNo);
				pstmt2.setDate(3, new Date(System.currentTimeMillis()));
				pstmt2.executeUpdate();
			}
			else {
				throw new IllegalArgumentException("不支持的状态类型 ：" + status);
			}
			
			conn.commit();	//提交事务
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			try{
                  conn.rollback();
                  conn.setAutoCommit(true);
			} 
			catch (SQLException e1){
				logger.error("数据回滚失败: no=" + houseNo + "  status=" + status + "  原因：" + e1.getMessage(),e1);
            }
			logger.error("更新失败: no=" + houseNo + "  status=" + status + "  原因：" + e.getMessage(),e);
		}		
	}
	
	/**
	   *  在传统浏览器中判断是否存在
	 * @return
	 */
	private int checkHouseOnLine(PCData item,WebDriver driver) {
		String url = "https://tj.lianjia.com/ershoufang/" + item + ".html";
		try {
			driver.get(url);			
			
		} catch (TimeoutException e) {			
			driver.navigate().to(url);			
		}
		
		int status = -1;
		WebElement tag = null;
		try {			
			tag = driver.findElement(By.xpath("/html/body/div[3]/div/div/div[1]/h1[@class='main']"));			
			status = HouseScratch.STATUS_ONLINE;			
			tag = driver.findElement(By.xpath("/html/body/div[3]/div/div/div[1]/h1[@class='main']/span"));
			if (null != tag && "已下架".equals(tag.getText())) {
				status = HouseScratch.STATUS_OFFLINE;
			}
		}
		catch(NoSuchElementException e) {
			if (HouseScratch.STATUS_ONLINE != status) {
	            int statusCode = RestAssured.get(url).statusCode();
	            if(200 != statusCode) {
	            	status = HouseScratch.STATUS_DEL;
	            	if(404 != statusCode)
	            		logger.info("未知状态:" + statusCode + " url:" + url);
	            }
	            else {
	            	logger.error("不能识别的网页格式，格式已变化。" + item + ".png");
	            	save2PNG(item, driver,PNG_TYPE_PUZZLE);
	            }
			}
			
		}
		catch (Exception e) {
			if (HouseScratch.STATUS_ONLINE != status) {
				int statusCode = RestAssured.get(url).statusCode();
				logger.info("未知错误状态:" + statusCode + " url:" + url);	            
			}			
		}

		driver.manage().deleteAllCookies();
		return status;
	}
	
	
	/**
	   *  在移动浏览器中判断是否存在
	 * @return
	 */
	private int checkHouseOnLineInMobile(PCData item,WebDriver driver) {
		String url = "https://m.lianjia.com/tj/ershoufang/" + item + ".html";
		try {
			driver.get(url);			
		} catch (TimeoutException e) {			
			logger.error(item + " 超时异常，ThreadId= " + Thread.currentThread().getId());
			return HouseScratch.STATUS_UNKNOWN;
		}
		
		int status = HouseScratch.STATUS_UNKNOWN;
		WebElement tag = null;
		try {			
			String xPath = "/html/body/div[1]/div/section/div[1]/div[1]/div[1]";
			tag = driver.findElement(By.xpath(xPath + "/ul"));			
			status = HouseScratch.STATUS_ONLINE;			
			tag = driver.findElement(By.xpath(xPath + "/img[@class='remove_tag']"));
			if (null != tag) {
				status = HouseScratch.STATUS_OFFLINE;
			}
		}
		catch (NoSuchElementException e) {
			if (HouseScratch.STATUS_ONLINE != status) {
				try{
					tag = driver.findElement(By.xpath("/html/body/div/section/div/img"));
					if (null != tag) {
						status = HouseScratch.STATUS_DEL;
					}
				}
				catch(NoSuchElementException ex){
					int statusCode = RestAssured.get(url).statusCode();
					if (200 != statusCode) {
						status = HouseScratch.STATUS_DEL;
						if (404 != statusCode)
							logger.info("未知Response状态:" + statusCode + " url:" + url);
					} else {
						logger.error("不能识别的网页格式，格式已变化。" + item + ".png");
						save2PNG(item, driver, PNG_TYPE_PUZZLE);
					}
					status = HouseScratch.STATUS_UNKNOWN;
				}
			}
		}

		//driver.manage().deleteAllCookies();
		return status;
	}

	/**
	 * 生成PNG截图
	 */
	private void save2PNG(PCData item, WebDriver driver, String type) {
		File screenshotFile = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
		try {
			FileUtils.copyFile(screenshotFile,new File("../logs/" + DateFormatUtils.format(new java.util.Date(), "yyyy-MM-dd_HHmmss_") +
					item + "_" + type.trim() + ".png"));
		} catch (IOException e) {
			logger.error("生成文件截图失败 No. " + item);
		}
	}
	
	public CountDownLatch getmDoneSignal() {
		return mDoneSignal;
	}

	public void setmDoneSignal(CountDownLatch mDoneSignal) {
		this.mDoneSignal = mDoneSignal;
	}

	public List<PCData> getQueue() {
		return queue;
	}

	public void setQueue(List<PCData> queue) {
		this.queue = queue;
	}

}
