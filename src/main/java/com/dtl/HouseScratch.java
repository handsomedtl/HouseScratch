package com.dtl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.openqa.selenium.chrome.ChromeDriverService;

import com.dtl.item.PCData;
import com.dtl.thread.ConsumerCallableThread;
import com.dtl.thread.ProducerCallableThread;
import com.dtl.util.RandomUtil;



public class HouseScratch {
	private static final String BROWSER_DRIVER = "webdriver.chrome.driver";
	private static Logger logger = Logger.getLogger(HouseScratch.class);  
	
	public static final int STATUS_UNKNOWN = -1;//未知状态，仅运于处理过程中的中间状态识别
	public static final int STATUS_ONLINE = 0;	//在线状态
	public static final int STATUS_OFFLINE = 1;	//已下线
	public static final int STATUS_DEL = 2;		//已删除
	public static final int STATUS_REONLINE = 3;//重新上线
	
	public static ReentrantLock lock = new ReentrantLock();
    public static Condition empty = lock.newCondition();
    public static Condition full = lock.newCondition();
	
	public static ResourceBundle resource;
	
	private Connection conn;
	private Statement stmt;
	private ChromeDriverService service;
	
	private Integer loadSizeforEverytime;	//每一线程每次批量扫描网页个数
	private Integer threadCount;			//设定生产者线程个数
	private ArrayList<String> indexArray;
	
	
	static {		
		/**
         * log4j.properties变量的值在脚本bin/run.sh 中读取
         * 注意一定要从环境变量读取
         */
        String log4fConfig = System.getProperty("log4j.properties");
		if (log4fConfig != null) {
			PropertyConfigurator.configure(log4fConfig);
			logger.info("------------- 程序开始初始化... -------------");
			logger.info("从" + log4fConfig + " 读取配置文件");
		} 
		else {
			logger.info("------------- 程序开始初始化... -------------");
			logger.info("从packet内读取log4j.properties配置文件");
		}
	}
	
	public HouseScratch() throws IOException {
		initialContext();
		
		loadSizeforEverytime = Integer.valueOf(resource.getString("siglethread.loadsize"));
		threadCount = Integer.valueOf(resource.getString("thread.count"));
		indexArray = RandomUtil.getDiffNO(10);
	}	

	private void initialContext() throws IOException {
		
		String configPath = System.getProperty("config.properties");
        String driverPath = null; 
		if(null == configPath) {
			// 使用packet内配置文件
			resource = ResourceBundle.getBundle("config");
			driverPath = resource.getString(BROWSER_DRIVER); 
			logger.info("从packet内读取config.properties配置文件");
        }
		else {
			BufferedReader bufferedReader = null;
			try {
				bufferedReader = new BufferedReader(new FileReader(configPath));
				resource = new PropertyResourceBundle(bufferedReader);
				
				logger.info("从 "+ configPath  + " 读取配置文件");
			} catch (IOException e) {
				logger.error("读取" + configPath +" 失败，" + e.getMessage());
			}
			finally {
				if(null != bufferedReader)
					bufferedReader.close();
			}
			driverPath = resource.getString(BROWSER_DRIVER); 
		}
		logger.info("DriverPath: " + driverPath);
		
		System.setProperty(BROWSER_DRIVER, driverPath);
		
		service = new ChromeDriverService.Builder().usingDriverExecutable(new File(driverPath)).usingAnyFreePort().build();
		service.start();
	}
	
	public void closeBrowser() {
		if(null != service) {
			service.stop();
		}
	}	
	
	public Connection initDB() {
		// 连接MySql数据库，用户名和密码都是root
		String jdbcUrl = resource.getString("jdbc.url");
		String userName = resource.getString("jdbc.username");
		String passWord = resource.getString("jdbc.password");		
		
		try {
			//加载MySql的驱动类   
		    Class.forName(resource.getString("jdbc.driver")) ;   
			conn = DriverManager.getConnection(jdbcUrl, userName, passWord);
			
			stmt = conn.createStatement() ;   
		} 
		catch (SQLException se) {
			logger.error("数据库连接失败！",se);
		} catch (ClassNotFoundException e) {
			logger.error("找不到驱动程序类 ，加载驱动失败！",e);
		}

		return conn;
	}
	
	/**
	 * 查询待处理的房子信息
	 * @param status 0:检查未下架的房子信息  !=0:查询哪些房子会重新上架 
	 */
	private List<PCData> getHouses(int status) {
		if(null == conn)
			initDB();
		
		ResultSet rs = null;
		String sql = (status == STATUS_ONLINE) ? resource.getString("Query_Sql") : resource.getString("ReQuery_Sql");
		List<PCData> noList = new ArrayList<>(10000);
		try {
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				PCData data = PCData.create();
				data.setHouseId(rs.getString("no"));
				data.setStatus(rs.getInt("status"));
				noList.add(data);
			}
		} catch (SQLException e) {
			logger.error(sql + "查询失败:" + e);
		}
		finally {
			if (rs != null) { // 关闭记录集
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("关闭数据记录集失败",e);
				}
			}
		}

		return noList;
	}	
	
	private void closeDB() {
		
		if (stmt != null) { // 关闭声明
			try {
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
				logger.error("数据库查询对象关闭失败");
			}
		}
		if (conn != null) { // 关闭连接对象
			try {
				conn.close();
				conn = null;
			} catch (SQLException e) {
				logger.error("数据库链接关闭失败");
			}
		}
	}
	
	public void startCheck(int checkType) {
		long startTime = System.currentTimeMillis();

		int threadCount = Integer.valueOf(resource.getString("thread.count"));

		List<PCData> list = scanRange(checkType);
		
		int loadSizeforEverytime = Integer.valueOf(resource.getString("siglethread.loadsize"));
		int tabCount = list.size() / loadSizeforEverytime;	//浏览器需要打开的TAB页个数
		
		CountDownLatch mDoneSignal = new CountDownLatch(tabCount + 1); // 需包含一个消费者线程
		OperatorService operatorService = new OperatorService(service, mDoneSignal, conn, list.size());
		ArrayList<String> indexArray =  RandomUtil.getDiffNO(10);
		
		
		ExecutorService executorService = Executors.newFixedThreadPool(threadCount+1);
		
		ConsumerThread consumerThread = new ConsumerThread(operatorService);				
		executorService.execute(consumerThread);
		
		for (int i = 0; i < tabCount; i++) {
			int endIndex = list.size();

			if (i * loadSizeforEverytime + loadSizeforEverytime < list.size())
				endIndex = i * loadSizeforEverytime + loadSizeforEverytime;
			if (i == tabCount - 1)
				endIndex = list.size();

			ProducerThread producerThread = new ProducerThread(list, i * loadSizeforEverytime, endIndex, operatorService,
					Integer.valueOf(indexArray.get(i % 10)));
			try {
				Thread.sleep(1000*Integer.valueOf(indexArray.get(i % 10)));
				executorService.execute(producerThread);
				
			} catch (NumberFormatException | InterruptedException e) {				
				e.printStackTrace();
			}
			
			if (endIndex == list.size())
				break;
		}
		
		
		try  
		{  
			mDoneSignal.await();// 等待所有工作线程结束
			logger.info(String.format("共有 %d 套房子已下架 ，%d 套被删除，%d 套重新上线，共耗时 %.1f分钟",OperatorService.offLineCount.get(),OperatorService.deleteCount.get(), OperatorService.reOnLineCount.get(), (System.currentTimeMillis()-startTime)/60000.0f));
			operatorService.saveStatistics();	
			closeBrowser();
			closeDB();
			
			executorService.shutdown();			
			if(!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)){  
	            // 超时的时候向线程池中所有的线程发出中断(interrupted)。  
				executorService.shutdownNow();  
	        }
			
			logger.info("房子信息处理完毕，程序退出\n\n");
			
			if("true".equalsIgnoreCase(resource.getString("shutdown"))) {
				//关闭计算机
				Runtime.getRuntime().exec("shutdown /s /t 50");
			}			
		}  
		catch (InterruptedException e){  
			executorService.shutdownNow();
			logger.error("线程异常终止",e);
		}
		catch(Exception e) {
			logger.error("线程异常终止",e);
		}
		finally {
			closeDB();
		}
	}	
	
	public void startCheckNew(int checkType) {
		long startTime = System.currentTimeMillis();
		
		List<PCData> list = scanRange(checkType);
		CountDownLatch mDoneSignal = new CountDownLatch(threadCount + 1); // 需包含一个消费者线程		
		
		OperatorService operatorService = new OperatorService(service, mDoneSignal, conn, list.size());
		
		ExecutorService executorService = Executors.newFixedThreadPool(threadCount+1);		
		ConsumerCallableThread consumerThread = new ConsumerCallableThread(operatorService);				
		Future<Boolean> consumeTaskResult = executorService.submit(consumerThread);
		
		List<PCData> taskList = new ArrayList<>();
		taskList.addAll(list);
		
		int reCount = 0;
		while(!taskList.isEmpty()){
			taskList = createProducerThread(operatorService, executorService, taskList);
			if(!taskList.isEmpty() && reCount <= 3){	//最多重新扫描三次
				logger.info(reCount + ": 部分失败的房源将会重新扫描");
				if(taskList.size() > Integer.valueOf(HouseScratch.resource.getString("queue.size")) &&
					taskList.size() <= Integer.valueOf(resource.getString("siglethread.loadsize")) * threadCount)
					loadSizeforEverytime = taskList.size() / threadCount; //任务各线程均分
				
				++reCount;
			}
			else if(reCount >= 3){
				logger.info("重新扫描次数已达到3次，终止扫描，所有生产者线程退出");
				consumeTaskResult.cancel(true);
				break;
			}
		}
		
		try  
		{			
			//等待消费者线程结束
			if(!consumeTaskResult.isCancelled())
				while(null == consumeTaskResult.get());
			
			logger.info(String.format("共有 %d 套房子已下架 ，%d 套被删除，%d 套重新上线，共耗时 %.1f分钟",OperatorService.offLineCount.get(),
					OperatorService.deleteCount.get(), OperatorService.reOnLineCount.get(), (System.currentTimeMillis()-startTime)/60000.0f));
			operatorService.saveStatistics();	
			closeBrowser();
			closeDB();
			
			executorService.shutdown();			
			if(!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)){  
	            // 超时的时候向线程池中所有的线程发出中断(interrupted)。  
				executorService.shutdownNow();  
	        }
			
			logger.info("房子信息处理完毕，程序退出\n\n");
			
			if("true".equalsIgnoreCase(resource.getString("shutdown"))) {
				//关闭计算机
				Runtime.getRuntime().exec("shutdown /s /t 60");
			}			
		}  
		catch (InterruptedException e){  
			executorService.shutdownNow();
			logger.error("线程异常终止",e);
		}
		catch(Exception e) {
			logger.error("线程异常终止",e);
		}
		finally {
			closeDB();
		}
	}

	private List<PCData> createProducerThread(OperatorService operatorService, ExecutorService executorService,
			List<PCData> taskList) {
		
		ArrayList<Future<List<PCData>>> errorTaskList = new ArrayList<>();
		
		int tabCount = 1;
		if(taskList.size() <= loadSizeforEverytime)
			tabCount = 1;
		else
			tabCount = taskList.size() / loadSizeforEverytime;	//浏览器需要打开的TAB页个数
		
		for (int i = 0; i < tabCount; i++) {
			int endIndex = taskList.size();

			if (i * loadSizeforEverytime + loadSizeforEverytime < taskList.size())
				endIndex = i * loadSizeforEverytime + loadSizeforEverytime;
			
			if (i == tabCount - 1)
				endIndex = taskList.size();

			ProducerCallableThread producerThread = new ProducerCallableThread(taskList, i * loadSizeforEverytime, endIndex, operatorService,
					Integer.valueOf(indexArray.get(i % 10)));
			try {
				Thread.sleep(1000*Integer.valueOf(indexArray.get(i % 10)));
				errorTaskList.add(executorService.submit(producerThread));
				
			} catch (NumberFormatException | InterruptedException e) {				
				e.printStackTrace();
			}
			
			if (endIndex == taskList.size())
				break;
		}
		
		List<PCData> errTaskList = new ArrayList<PCData>();
		
		// 等待所有生产者工作线程结束
		for (Future<List<PCData>> fs : errorTaskList) {  
			try {
				errTaskList.addAll(fs.get());
			} 
			catch (InterruptedException e) {
				logger.error(e.getMessage());
			} 
			catch (ExecutionException e) {
				logger.error(e.getMessage());
			}
			catch(Exception e){
				logger.error(e.getMessage());
			}
		}		
		return errTaskList;
	}

	/**
	 * 根据用户输入设定房源扫描范围
	 */
	private List<PCData> scanRange(int checkType) {
		List<PCData> list;
		
		switch(checkType){
		case 2:
			logger.info("-----选择仅扫描目前在线房源信息-----");
			list = getHouses(STATUS_ONLINE);
			break;
		case 3:
			logger.info("-----选择仅扫描历史下线房源信息-----");
			list = getHouses(STATUS_OFFLINE);
			break;
		default:
			logger.info("-----选择扫描所有以及包含历史已下线房源信息-----");
			list = getHouses(STATUS_ONLINE);
			list.addAll(getHouses(STATUS_OFFLINE));
		}
		
		if (list.isEmpty()) {
			logger.error("☹ 未获得任何房子ID ☹");
		}
		
		logger.info(String.format("共有%d套房源将会扫描\n", list.size()));
		return list;
	}
	
	
	/**
	 * 程序入口
	 */
	public static void main(String[] args) {		
		logger.info("版本信息 v1.2.0  Build 2019.08.13\n");
		
		HouseScratch hs;
		System.out.println("\n请输入是否扫描房源类别：1:所有房源  \t 2:仅在线房源\t 3:仅历史下线房源");
		Scanner sc = new Scanner(System.in);
		String checkType = sc.next().trim();
		sc.close();
		
		try {
			hs = new HouseScratch();
			hs.startCheckNew(Integer.valueOf(checkType));
		} 
		catch (Exception e) {
			logger.error("ChromeDrvier启动失败，程序退出", e);
		}
	}
}
