package it.uniroma1.di.fff.Util;

import it.uniroma1.di.fff.QkCount.QkCountDriver;



public class ClockTimeLogger {
	
	private long clockTime;
	
//	private final Log LOG;
	private String log;
	private String id;
	
	public ClockTimeLogger(String log, String id) {
		this.log=log;
		this.id=id;
//		this.LOG = LogFactory.getLog(QkCountDriver.TIME_LOG_PREAMBLE + " " + log );
		this.clockTime = System.nanoTime();
	}
	
	/*public void logCpuTime(String additionalInfo) {
		cpuTime = thread.getCurrentThreadCpuTime() - cpuTime;
		LOG.info(this.id + QkCountDriver.TIME_LOG_SEPARATOR + additionalInfo + QkCountDriver.TIME_LOG_SEPARATOR + this.cpuTime);
	}*/
	public void logClockTime(String inputSize, String outputSize) {
		clockTime = System.nanoTime()- clockTime;
		/*LOG.info*/
		System.out.println(
				this.log + QkCountDriver.TIME_LOG_SEPARATOR +
				this.id + QkCountDriver.TIME_LOG_SEPARATOR + 
				/*inputSize + QkCountDriver.TIME_LOG_SEPARATOR + 
				outputSize + QkCountDriver.TIME_LOG_SEPARATOR +*/
				this.clockTime);
	}
	
	public void logClockTime() {
		this.logClockTime("-","-");
	}
}
