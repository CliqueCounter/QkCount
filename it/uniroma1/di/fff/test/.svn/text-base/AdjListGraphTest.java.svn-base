package it.uniroma1.di.fff.test;

import it.uniroma1.di.fff.Util.AdjListGraph;

import java.io.BufferedReader;
import java.io.FileReader;

public class AdjListGraphTest {

	public static void main (String [] args) {
		AdjListGraph  g = new AdjListGraph();

		
		String line = null;
		String [] nodes;
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			//for (int i = 0; i < 4; i++) line = reader.readLine();

			while ((line = reader.readLine()) != null) {

				if(!line.startsWith("#")) {
					nodes = line.split("\t");
					if(nodes[0].compareTo(nodes[1]) < 0) {
						g.addEdge(nodes[0], nodes[1]);
						//System.out.println("added " + nodes[0] + " "  + nodes[1]);
					} else {
						g.addEdge(nodes[1],nodes[0]);
						//System.out.println("added " + nodes[1] + " "  + nodes[0]);
					}
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
