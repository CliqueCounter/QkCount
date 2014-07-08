/* ============================================================================
 *  CommandComputeTriangles.java
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

@Parameters(separators = "=", commandDescription = "Count the number of triangles" ) 
public class CommandCountTriangles extends AbstractCommand {
	
	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_USE_LAZY_PAIRS,
			description = "Use the lazy pair explosion",
			required = false, 
			validateWith = ParametersValidator.class,
			arity = 1
			)
	private
	boolean useLazyPairs = false;

	public boolean getUseLazyPairs() {
		return useLazyPairs;
	}

	public void setUseLazyPairs(boolean useLazyPairs) {
		this.useLazyPairs = useLazyPairs;
	}
	
	
}

