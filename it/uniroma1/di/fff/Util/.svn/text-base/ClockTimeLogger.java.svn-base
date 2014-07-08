/* ============================================================================
 *  ClockTimeLogger.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		utility class for logging wall-clock time
 *  					
 */

package it.uniroma1.di.fff.Util;

import java.text.DecimalFormat;

import it.uniroma1.di.fff.QkCount.QkCountDriver;



public class ClockTimeLogger {
	
	private long clockTime;
	
	private String log;
	private String id;
	
	public ClockTimeLogger(String log, String id) {
		this.log=log;
		this.id=id;
		this.clockTime = System.nanoTime();
	}
	
	/*public void logCpuTime(String additionalInfo) {
		cpuTime = thread.getCurrentThreadCpuTime() - cpuTime;
		LOG.info(this.id + QkCountDriver.TIME_LOG_SEPARATOR + additionalInfo + QkCountDriver.TIME_LOG_SEPARATOR + this.cpuTime);
	}*/
	public void logClockTime(String inputSize, String outputSize) {
		clockTime = System.nanoTime()- clockTime;
		/*LOG.info*/
		
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		System.out.println(
				this.log + QkCountDriver.TIME_LOG_SEPARATOR +
				this.id + QkCountDriver.TIME_LOG_SEPARATOR + 
				/*inputSize + QkCountDriver.TIME_LOG_SEPARATOR + 
				outputSize + QkCountDriver.TIME_LOG_SEPARATOR +*/
				df.format((float)this.clockTime/60000000000f));
	}
	
	public void logClockTime() {
		this.logClockTime("-","-");
	}
}
