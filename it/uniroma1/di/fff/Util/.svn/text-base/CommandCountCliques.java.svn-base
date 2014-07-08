/* ============================================================================
 *  CommandCountCliques.java
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

@Parameters(separators = "=", commandDescription = "Count the number of cliques of the given size" ) 
public class CommandCountCliques extends AbstractCommand {
	
	@Parameter(
			names =  QkCountDriver.COMMAND_PARAM_CLIQUE_SIZE, 
			description = "Size of the cliques to be counted", 
			required = true, 
			validateWith = ParametersValidator.class
			)
	private
	int cliqueSize = 4;
	
	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_EDGE_SAMPLE, 
			description = "Use edge sampling with the given probability", 
			validateWith = ParametersValidator.class
			)
	private
	double edgeSamplingProbability = -1;

	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_COLOR_SAMPLE, 
			description = "Use coloring based sampling with the given number of colors", 
			validateWith = ParametersValidator.class
			)
	private
	int colorSampleColors = -1;
	
	@Parameter(
			names = QkCountDriver.COMMAND_PARAM_USE_L_PLUS_N,
			description = "Use the LPlusN clique count algorithm if true, NodeIterator++ if false",
			required = false, 
			validateWith = ParametersValidator.class,
			arity = 1
			)
	private
	boolean useLPlusN = true;
	
	public boolean getUseLPlusN() {
		return useLPlusN;
	}

	public void setUseLPlusN(boolean useLPlusN) {
		this.useLPlusN = useLPlusN;
	}
	
	/*@Parameter(
			names=QkCountDriver.COMMAND_PARAM_ITERATE_ABOVE_SIZE,
			description = "Solve large subgraph by iterating the overall algorithm",
			validateWith = ParametersValidator.class
			)
	private long graphSizeForIteration = -1;*/

	

	public double getEdgeSamplingProbability() {
		return edgeSamplingProbability;
	}

	public void setEdgeSamplingProbability(double edgeSamplingProbability) {
		this.edgeSamplingProbability = edgeSamplingProbability;
	}

	public int getColorSampleColors() {
		return colorSampleColors;
	}

	public void setColorSampleColors(int colorSampleColors) {
		this.colorSampleColors = colorSampleColors;
	}

	public int getCliqueSize() {
		return cliqueSize;
	}

	public void setCliqueSize(int cliqueSize) {
		this.cliqueSize = cliqueSize;
	}
	
	/*public long getGraphSizeForIteration () {
		return this.graphSizeForIteration;
	}
	
	public void setGraphSizeForIteration (long graphSizeForIteration) {
		this.graphSizeForIteration= graphSizeForIteration;
	}*/
}

