package it.uniroma1.di.fff.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class AdjListGraph {

	private long unorientedSize=0;
	private long orientedSize=0;
	//private int nNodes=0;

	HashMap<String, HashSet<String>> graph = new HashMap<String, HashSet<String>>();  
	HashMap<String, Integer> degrees =new HashMap<String, Integer>();
	//HashMap<String, ArrayList<String>> largerNeighbors = new HashMap<String,ArrayList<String>>();

	class AdjListGraphNodeComparator implements Comparator<String> {

		AdjListGraph g;

		AdjListGraphNodeComparator(AdjListGraph g){
			this.g = g;
		}

		@Override
		public int compare(String a, String b) {

			if(Checker.DoubleCheck(a,g.getNodeDegree(a), b, g.getNodeDegree(b)))
				return -1;

			return 1;
		}

	}
	public ArrayList<String> getNodeList() {
		ArrayList<String> l = new ArrayList<String>(graph.keySet());

		return l; 
	}

	public int getNodesNumber() {
		return this.getNodeList().size();
	}

	public ArrayList<String> getNodeListSorted() {
		ArrayList<String> l = this.getNodeList();
		Collections.sort(l,this.new AdjListGraphNodeComparator(this));

		return l; 
	}


	public ArrayList<String> getLargerNeighbors(String a) {

		ArrayList<String> filtered; // =  largerNeighbors.get(a);

		//if(filtered==null) {
			filtered =  new ArrayList<String>();
		//	largerNeighbors.put(a, filtered);

			HashSet<String>neigh = graph.get(a);



			if(neigh != null) {


				Iterator<String> it = neigh.iterator();

				int aDeg= this.getNodeDegree(a);

				while(it.hasNext()){
					String neighbor = it.next();
					if(Checker.DoubleCheck(a,aDeg,neighbor, this.getNodeDegree(neighbor))) {
						filtered.add(neighbor);
					}
				}

				//Collections.sort(filtered,this.new AdjListGraphNodeComparator(this));
			//}
		}

		return filtered; 
	}


	/*public ArrayList<String> getLargerNeighborsSortedByDegree(String a) {

		ArrayList<String> filtered =  this.getLargerNeighbors(a);
		Collections.sort(filtered,this.new AdjListGraphNodeComparator(this));
		return filtered; 
	}*/

	private boolean checkPreviousAreAdjacent(ArrayList<String> nodes, int [] indexes, int upTo) {
		for (int i = 0; i< upTo; i++) {
			if(!this.hasNeighbor(nodes.get(indexes[i]),  nodes.get(indexes[upTo]))) return false;
		}
		return true;
	}

	public long countCliquesOfSize(int cliqueSize) {

		ArrayList<String> l = this.getNodeList();
		ArrayList<String> neighbors; 

		long countRunning=0;

		Iterator<String> it = l.iterator();		


		int [] indexes = new int[cliqueSize-1];


		String a;
		while(it.hasNext()){
			a=it.next();

			neighbors = this.getLargerNeighbors/*SortedByDegree*/(a);

			if(neighbors.size() >= cliqueSize-1) {

				indexes[0] = 0;
				int fixing = 0;
				boolean failure = false;


				while(fixing>=0) {

					while (!failure) {

						while(!failure && !this.checkPreviousAreAdjacent(neighbors, indexes, fixing)){

							indexes[fixing]++;
							if (!(indexes[fixing]<(neighbors.size()-(indexes.length  - fixing - 1)))) {
								failure=true;

							}
						}
						if(!failure) {
							if (fixing+1<indexes.length) {
								fixing++;
								indexes[fixing] = indexes[fixing-1]+1;
							} else {
								countRunning++;
								indexes[fixing]++;
							}
							if (!(indexes[fixing]<(neighbors.size()-(indexes.length  - fixing - 1)))) {
								failure=true;
							}
						}
					}

					fixing--;
					if(fixing>=0) {
						indexes[fixing]++;
						failure=(!(indexes[fixing]<(neighbors.size()-(indexes.length  - fixing - 1))));
					}
				}
			}
		}

		return countRunning;

	}

	@SuppressWarnings("unused")
	private long countTriangles () {

		ArrayList<String> l = this.getNodeList();
		ArrayList<String> neighbors; 

		long countRunning=0;

		Iterator<String> it = l.iterator();		

		String a,b,c;

		while (it.hasNext()){
			a = it.next();


			neighbors = this.getLargerNeighbors/*SortedByDegree*/(a);

			for(int i = 0; i<neighbors.size();i++) {
				b = neighbors.get(i);
				for (int j =i+1;j<neighbors.size();j++){
					c=neighbors.get(j);
					if(this.hasNeighbor(b, c)) countRunning++;
				}
			}


		}
		return countRunning;
	}


	int getNodeDegree(String a) {
		Integer deg = degrees.get(a);
		if(deg == null) return 0;
		return deg;
	}
	void setNodeDegree(String a, int deg){
		degrees.put(a,deg);
	}	
	public boolean addEdge (String a, String b) {

		boolean  res = this.addOrientedEdge(a,b)|this.addOrientedEdge(b,a);
		if (res) {
			this.unorientedSize++;
		}
		return res;

	}


	boolean addOrientedEdge(String source, String target) {

		boolean res = false;
		HashSet<String> adj;

		if (!graph.containsKey(source)) {
			adj = new HashSet<String>();
			graph.put(source, adj);
		} else {
			adj = graph.get(source);
		}
		if(!adj.contains(target)){
			adj.add(target);
			setNodeDegree(source, 1+getNodeDegree(source));
			this.orientedSize++;
			res = true;
		}
		return res;
	}

	boolean removeEdge (String a, String b) {

		boolean res = this.removeOrientedEdge(a,b)|this.removeOrientedEdge(b,a);
		if(res) {
			this.unorientedSize--;
		}
		return res;
	}

	boolean hasNeighbor(String source, String target) {

		return (graph.containsKey(source)&&graph.get(source).contains(target));
	}

	boolean removeOrientedEdge(String source, String target) {

		HashSet<String> adj;

		if (graph.containsKey(source)) {
			adj = graph.get(source);
			if(adj.contains(target)){	
				adj.remove(target);
				if(adj.isEmpty()){
					graph.remove(source);
					degrees.remove(source);
				} else {
					setNodeDegree(source, getNodeDegree(source)-1);
				}
				this.orientedSize--;
				return true;
			}
		}
		return false;
	}

	public long getUnorientedSize() {
		return unorientedSize;
	}

	/*public void setUnorientedSize(long unorientedSize) {
		this.unorientedSize = unorientedSize;
	}*/

	public long getOrientedSize() {
		return orientedSize;
	}

	/*public void setOrientedSize(long orientedSize) {
		this.orientedSize = orientedSize;
	}*/


}
