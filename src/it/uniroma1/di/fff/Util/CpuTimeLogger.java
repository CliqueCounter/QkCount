package it.uniroma1.di.fff.Util;

import it.uniroma1.di.fff.QkCount.QkCountDriver;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;



public class CpuTimeLogger {

	private long cpuTime;
	private ThreadMXBean thread;
//	private final Log LOG;
	private String log;
	private String id;
	private boolean doLog;

	public CpuTimeLogger(String log, String id, boolean doLog) {
		this.id=id;
//		this.LOG = LogFactory.getLog(QkCountDriver.TIME_LOG_PREAMBLE + " " + log );
		this.log=log;
		this.thread = ManagementFactory.getThreadMXBean();
		this.cpuTime=thread.getCurrentThreadCpuTime();
		this.doLog=doLog;
	}

	/*public void logCpuTime(String additionalInfo) {
		cpuTime = thread.getCurrentThreadCpuTime() - cpuTime;
		LOG.info(this.id + QkCountDriver.TIME_LOG_SEPARATOR + additionalInfo + QkCountDriver.TIME_LOG_SEPARATOR + this.cpuTime);
	}*/
	public void logCpuTime(String inputSize, String outputSize) {

		cpuTime = thread.getCurrentThreadCpuTime() - cpuTime;
		if(doLog /*|| cpuTime>=100000000*/) {
			//LOG.info(
			System.out.println(QkCountDriver.TIME_LOG_PREAMBLE + log + " " +
					this.id + QkCountDriver.TIME_LOG_SEPARATOR + 
					inputSize + QkCountDriver.TIME_LOG_SEPARATOR + 
					outputSize + QkCountDriver.TIME_LOG_SEPARATOR +
					this.cpuTime);
		}
	}

	/*public void logCpuTime() {
		cpuTime = thread.getCurrentThreadCpuTime() - cpuTime;
		LOG.info(this.id + QkCountDriver.TIME_LOG_SEPARATOR + this.cpuTime);
	}*/
}
