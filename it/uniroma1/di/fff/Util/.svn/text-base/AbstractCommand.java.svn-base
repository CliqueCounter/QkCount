/* ============================================================================
 *  AbstractCommand.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Abstract class that groups common parameters of different commands
 *  					
*/

package it.uniroma1.di.fff.Util;

import it.uniroma1.di.fff.QkCount.QkCountDriver;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=", commandDescription = "Abstract Command" ) 
public class AbstractCommand {
	
	final int ToMegabytes = 1024*1024;
	
	@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_LOG_CPUTIME, 
			description = "enable/disable detailed mappers and reducers cpu usage logs", 
			required = false, 
			validateWith = ParametersValidator.class,
			arity = 1
			)
	
	private 
	boolean logCpuTime=false;
	
	public boolean getLogCpuTime() {
		return logCpuTime;
	}

	public void setLogCpuTime(boolean logCpuTime) {
		this.logCpuTime = logCpuTime;
	}

	@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_WORKING_DIR, 
			description = "HFS Working dir (full path)", 
			//required = true, 
			validateWith = ParametersValidator.class
			)
	private
	String workingDir = "/";

	public String getWorkingDir() {
		if (!this.workingDir.endsWith("/")) {
			this.workingDir = this.workingDir+"/";
		}
		return this.workingDir;

	}

	public void setWorkingDir(String workingDir) {
		if(workingDir.endsWith("/")) {
			this.workingDir = workingDir;
		} else {
			this.workingDir = workingDir + "/";
		}
	}

	/*@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_RUN_ON_AMAZON,
			description = "Execution on Amazon EMR (sets IO on s3 filesystem)", 
			required = false, 
			validateWith = ParametersValidator.class,
			 arity = 1
			)
	private boolean runOnAmazon = true;

	public boolean getRunOnAmazon() {
		return runOnAmazon;
	}

	public void setRunOnAmazon(boolean runOnAmazon) {
		this.runOnAmazon = runOnAmazon;
	}*/
	
	@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_FILE_IN,
			description = "Input File", 
			required = true, 
			validateWith = ParametersValidator.class
			)
	private
	String fileIn = "";

	public String getFileIn() {
		return fileIn;
	}

	public void setFileIn(String fileIn) {
		this.fileIn = fileIn;
	}
	@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_FILE_OUT,
			description = "Output File", 
			required = true, 
			validateWith = ParametersValidator.class
			)
	private
	String fileOut = "";

	public String getFileOut() {
		return fileOut;
	}

	public void setFileOut(String fileOut) {
		this.fileOut= fileOut;
	}
	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_REDUCERS,
			description = "Number of reducers",
			required = false,
			validateWith = ParametersValidator.class
			)
	private 
	int numReducers = 4;

	public int getNumReducers() {
		return numReducers;
	}

	public void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}
	
	@Parameter (
			names = QkCountDriver.COMMAND_PARAM_SPECULATIVE_EXEC,
			description = "Enabe/disable speculative execution",
			required = false,
			validateWith = ParametersValidator.class,
			arity = 1
			)
	private	boolean speculativeExecution = false;

	public boolean getSpeculativeExecution() {
		return speculativeExecution;
	}

	public void setSpeculativeExecution(boolean speculativeExecution) {
		this.speculativeExecution = speculativeExecution;
	}
	
	@Parameter (
			names = QkCountDriver.COMMAND_PARAM_SPLIT_SIZE,
			description = "Set the split size for input files",
			required = false,
			validateWith = ParametersValidator.class,
			arity = 1
			)
	int splitSize = 64; //64Mb

	public int getSplitSize() {
		return splitSize*ToMegabytes;
	}

	public void setSplitSize(int splitSize) {
		this.splitSize = splitSize;
	}
	
	
	
	
}