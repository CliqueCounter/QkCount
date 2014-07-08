/* ============================================================================
 *  CommandCountByJoin.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		utility class for defining args commands
 *  					
 */

package it.uniroma1.di.fff.Util;

import it.uniroma1.di.fff.QkCount.QkCountDriver;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=", commandDescription = "Count the number of cliques of the given size using multijoin" ) 
public class CommandCountByJoin extends AbstractCommand {

	@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_CLIQUE_SIZE, 
			description = "Size of the cliques to be counted", 
			required = true, 
			validateWith = ParametersValidator.class
			)
	private
	int cliqueSize = 4;


	public int getCliqueSize() {
		return cliqueSize;
	}

	public void setCliqueSize(int cliqueSize) {
		this.cliqueSize = cliqueSize;
	}
	
	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_BUCKETS_NUM,
			description = "Number of buckets to use for the multiway join",
			required = true,
			validateWith = ParametersValidator.class
			)
	int nBuckets = this.cliqueSize;

	public int getnBuckets() {
		return nBuckets;
	}

	public void setnBuckets(int nBuckets) {
		this.nBuckets = nBuckets;
	}

/*	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_USE_TABLES,
			description = "Use tables to count cliques insteat of L+n or nodeIterator++ inside reducers",
			required = false, 
			validateWith = ParametersValidator.class,
			arity = 1
			)
	boolean useTables = true;


	public boolean getUseTables() {
		return useTables;
	}

	public void setUseTables(boolean useTables) {
		this.useTables = useTables;
	}*/

}

