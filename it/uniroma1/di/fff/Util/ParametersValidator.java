/* ============================================================================
 *  ParametersValildator.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		utility class for checking constraints on command parameters
 *  						(missing many controls that are still to be implemented)
 *  					
 */


package it.uniroma1.di.fff.Util;

import it.uniroma1.di.fff.QkCount.QkCountDriver;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

//TODO: implement the missing validators

public class ParametersValidator implements IParameterValidator {

	@Override
	public void validate(String name, String value) throws ParameterException {

		switch (name) {
		case QkCountDriver.COMMAND_PARAM_COLOR_SAMPLE:

			try {
				int intValue=Integer.parseInt(value);
				if (intValue <1 ) {
					throw new ParameterException("The number of colors must be an integer larger than 1, found " + intValue);
				}	
			} catch (NumberFormatException e) {
				throw new ParameterException("The number of colors must be an integer larger than 1, found " + value + " which is not an integer number");
			}	

			break;

		case QkCountDriver.COMMAND_PARAM_EDGE_SAMPLE:

			try {
				double doubleValue=Double.parseDouble(value);
				if (doubleValue >=1 || doubleValue <=0 ) {
					throw new ParameterException("The edge sampling probability must be a number larger than 0 and less than 1, found " + doubleValue);
				}	
			} catch (NumberFormatException e) {
				throw new ParameterException("The edge sampling probability must be a number larger than 0 and less than 1, found " + value + " which is not a number");
			}	

			break;

		default:
			break;
		}

	}

}
