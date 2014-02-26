package it.uniroma1.di.fff.Util;

import it.uniroma1.di.fff.QkCount.QkCountDriver;

public class Checker {

	public static String[] splitNodeAndDegree(String nodeAndDegree) {
		//ArrayList<String> res =new ArrayList<String>();
		String[] res = nodeAndDegree.split(QkCountDriver.NODE_DEGREE_SEPARATOR);
		if(res.length != 2) return null; 
			//throw new Error("Wrong node-degree pair!");
		return res;
	}

	/* Metodo statico della classe che confronta due nodi e ritorna true      *
	     * se node1 precede node2 nell'ordinamento lineare basato sul grado.	  */
	public static boolean DoubleCheck(String node1,int degree_node1,String node2,int degree_node2) {
	
		if(degree_node1 < degree_node2)
			return true;
	
		else {
		
			if((degree_node1 == degree_node2) && ((node1.compareTo(node2)) < 0))
				return true;
		
			else return false;
		
		}
	
	}

	/* Metodo statico della classe che confronta tre nodi e ritorna true      *
		 * se node1 precede node2 e node3 nell'ordinamento lineare basato 		  *
		 * sul grado.	  														  */
	public static boolean TripleCheck(String node1,int degree_node1,String node2,int degree_node2,String node3, int degree_node3) {
		
		if((degree_node1 < degree_node2) && (degree_node1 < degree_node3))
			return true;
		
		else {
			
			if((degree_node1 == degree_node2) && (degree_node1 < degree_node3)) {
				
				if((node1.compareTo(node2)) < 0)
					return true;
				
				else return false;
				
			}
			
			else {
				
				if((degree_node1 < degree_node2) && (degree_node1 == degree_node3)) {
					
					if((node1.compareTo(node3)) < 0)
						return true;
					
					else return false;
					
				}
				
				else {
					
					if((degree_node1 == degree_node2) && (degree_node1 == degree_node3))
						
						if(((node1.compareTo(node2)) < 0) && ((node1.compareTo(node3))<0))
							return true;
							
						else return false;
						
					else return false;
					
				}
				
			}
			
		}
		
	}

}
