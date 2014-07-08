package it.uniroma1.di.fff.test;

import it.uniroma1.di.fff.Util.LPlusNGraph;

import java.io.BufferedReader;
import java.io.FileReader;

public class LPlusNGraphTest {

	public static void main (String [] args) {
		LPlusNGraph g = new LPlusNGraph();

		
		String line = null;
		String [] nodes;
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			//for (int i = 0; i < 4; i++) line = reader.readLine();

			while ((line = reader.readLine()) != null) {

				if(!line.startsWith("#")) {
					nodes = line.split("\t");
					//if((Math.abs(nodes[0].hashCode()) % 4) != (Math.abs(nodes[1].hashCode()) % 4)) {
						g.addEdge(nodes[0], nodes[1]);
					//}
				}
			}

			reader.close();

			int cliqueSize = Integer.parseInt(args[1]);

			System.out.println("There are " + g.countCliquesOfSize(cliqueSize) +  " cliques of size " + cliqueSize + " in graph " + args[0]);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(line);
		}
	}
}
