package it.uniroma1.di.fff.test;

import it.uniroma1.di.fff.Util.LPlusNGraphWithBuckets;

import java.io.BufferedReader;
import java.io.FileReader;

public class LPlusNGraphWithBucketsTest {

	public static void main (String [] args) {

		if (args.length < 4) {
			System.out.println("Usage: LPlusNGraphWithBucketsTest inputGraph nBuckets cliqueSize p1 p2 ... pk");
		}
		int nBuckets = Integer.parseInt(args[1]);
		int [] partition = new int [args.length-3];
		for (int i=0; i<partition.length; i++) {
			partition[i] = Integer.parseInt(args[i+3]);
		}

		LPlusNGraphWithBuckets g = new LPlusNGraphWithBuckets(nBuckets, partition);


		String line = null;
		String [] nodes;

		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			//for (int i = 0; i < 4; i++) line = reader.readLine();

			while ((line = reader.readLine()) != null) {

				if(!line.startsWith("#")) {
					nodes = line.split("\t");
					//if((Math.abs(nodes[0].hashCode()) % nBuckets) != (Math.abs(nodes[1].hashCode()) % nBuckets)) {
						
						g.addEdge(nodes[0], nodes[1]);
						
					//}
				}
			}


			reader.close();

			int cliqueSize = Integer.parseInt(args[2]);

			System.out.println("There are " + g.countCliquesOfSize(cliqueSize) +  " cliques of size " + cliqueSize + " in graph " + args[0] + " with partition " + partition.toString());

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(line);
		}
	}
}