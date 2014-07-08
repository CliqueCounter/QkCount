package it.uniroma1.di.fff.test;

import org.apache.hadoop.io.Text;

import it.uniroma1.di.fff.QkCount.URound1.Map;

public class URound1TestMapper {

	public static void main (String [] args) {
		
		int cliqueSize, nBuckets, i, j;
		
		if (args.length != 4) {
			System.out.println("Usage: URound1TestMapper cliqueSize nBuckets i j");
		}
		cliqueSize = Integer.parseInt(args[0]);
		nBuckets = Integer.parseInt(args[1]);
		i = Integer.parseInt(args[2]);
		j = Integer.parseInt(args[3]);
		
		//URound1 r1 = new URound1();
		
		Map m = new Map();
		
		m.dbgSetup(nBuckets, cliqueSize);
		m.testMap(new Text(Integer.toString(i)), new Text(Integer.toString(j))); 
		
		
	}
	
	
}
