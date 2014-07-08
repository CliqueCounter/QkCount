/* ============================================================================
 *  DoubleChecker.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		utility class for comparing nodes based on their degree
 *  					
 */


package it.uniroma1.di.fff.Util;

import java.util.Comparator;

public class DoubleChecker  implements Comparator<String> {
	
	@Override
	public int compare(String node1, String node2) {
		String[] n1 = Checker.splitNodeAndDegree(node1);
		String[] n2=  Checker.splitNodeAndDegree(node2);
		
		
		int deg1 = Integer.parseInt(n1[1]);
		int deg2 = Integer.parseInt(n2[1]);
		
		if(Checker.DoubleCheck(n1[0],deg1, n2[0], deg2))
			return -1;
		
		return 1;
	}

}