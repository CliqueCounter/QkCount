package it.uniroma1.di.fff.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

public  class LPlusNGraph implements CliqueCounterGraph {

	HashMap<String,Integer> nodeMapper = new HashMap<String,Integer>();
	int nNodes = 0;

	ArrayList<Node> nodes = new ArrayList<Node>(256);

	public int getNodesNumber() {
		return nNodes;
	}

	protected void prepareForCounting() {
		
		nodeMapper=null;
		
		LPlusNNodesFilter filter = new LPlusNNodesFilter();
		
		for (Node n : nodes) {
			filter.pruneNeighbors(n);
		}
		
		/*Comparator<Node> cmp = new LPlusNNodesComparatorDecreasing();
		for (Node n : nodes) {
			n.sortNeighbors(cmp);
		}*/
	}
	
	public long countCliquesOfSize(int cliqueSize) {

		//Debug
		//System.out.println("countCliquesOfSize(" + cliqueSize + ")");

		this.prepareForCounting();
		
		
		long res = 0;
		
		if(cliqueSize <2) throw new Error("Trying to count cliques of size 1 or less!");
		if (cliqueSize == 2) {
			for (Node n : nodes) {
				res+= n.getGammaPlus().size();
			}
			return res;
		}
		
		nodeMapper = null;

		ArrayList<Iterator<Node>> queues = new ArrayList<Iterator<Node>>(cliqueSize -2); 
		ArrayList<ArrayList<Node>> marked = new ArrayList<ArrayList<Node>>(cliqueSize -2);
		ArrayList<Node> list;

		for(int i = 0; i < cliqueSize-2; i++) {
			marked.add(i, null);
			queues.add(i, null);
		}


		
		byte mark,currentQueue;
		for (Node n : nodes) {
			

			mark = 1;
			currentQueue = 0;

			Node currentNode = n;

			list = new ArrayList<Node>(n.getGammaPlus().size());

			for (Node i : n.getGammaPlus()) {
				i.setMark(mark);
				list.add(i);

			}

			queues.set(currentQueue, list.iterator());
			marked.set(currentQueue, list);


			boolean foundEmpty = false;

			while (currentQueue >=0 && (foundEmpty || queues.get(currentQueue).hasNext())) {

				if (!foundEmpty) currentNode = queues.get(currentQueue).next();
				

				//ADDING NODES TO THE QUEUES AND MARKING
				if(mark<cliqueSize -2 && !foundEmpty) {
					
					mark ++;
					currentQueue ++;

					list = new ArrayList<Node>();
					marked.set(currentQueue, list);

					for (Node i : currentNode.getGammaPlus()) {


						if (i.getMark()==mark-1) {
							i.setMark(mark);

							list.add(i);
						}
					}

					if (list.isEmpty()) {
						//KILL THIS ITERATION OF THE LOOP - NO CLIQUES CAN BE FOUND
						foundEmpty = true;

					} else {

						queues.set(currentQueue,list.iterator());
					}
				} else {
					if(!foundEmpty) { //CHECK IF WE REACHED CLIQUE SIZE
						for (Node i : currentNode.getGammaPlus()) {

							if (i.getMark()==mark) {
								res++;
							}
						}
					}
					while (currentQueue >= 0 && (foundEmpty || !queues.get(currentQueue).hasNext())) {
						
						foundEmpty = false;
						
						mark --;
						for(Node toUnmark : marked.get(currentQueue)){
							toUnmark.setMark(mark);
						}
						currentQueue --;
					}

				}

			}

			for (Node i : n.getGammaPlus()) {
				i.setMark((byte) 0);
			}

		}

		return res;
	}
	@Override
	public void addEdge(String a, String b) {
		Node n1,n2;
		n1 = this.getNode(a);
		n2 = this.getNode(b);
		//nodes.get(n1.pos).addNeighbor(n2);
		//nodes.get(n2.pos).addNeighbor(n1);
		n1.addNeighbor(n2);
		n2.addNeighbor(n1);		

	}

	protected Node getNode(String nodeLabel) {

		Integer pos = nodeMapper.get(nodeLabel.toString());
		Node n;

		if(pos==null) {
			pos = new Integer(nNodes);
			n = new Node(nNodes/*, nodeLabel*/);
			nodeMapper.put(nodeLabel, pos);
			nodes.add(n); 
			nNodes ++;
		} else {
			n = nodes.get(pos);
		}
		return n;
	}

	/*protected Node getNode(int nIndex) {
		return nodes.get(nIndex);
	}*/


}


class Node {

	//String realLabel;
	protected ArrayList<Node> neighbors;
	//int gammaPlusIndex = -1;
	//LPlusNGraph graph;
	byte mark = 0;
	int degree = 0;
	int pos;


	public byte getMark() {
		return mark;
	}

	public void setMark(byte mark) {
		this.mark = mark;
	}

	protected Node (int pos/*, String realLabel*/) {
		//this.realLabel = realLabel;
		this.neighbors = new ArrayList<Node>();
		//this.graph = graph;
		this.pos=pos;
	}

	protected void addNeighbor (Node i) {
		this.neighbors.add(i);
		degree++;
	}
	protected int getDegree(){
		return degree;
	}

	protected ArrayList<Node> getGammaPlus() {
		return neighbors;
	}



	protected void sortNeighbors(Comparator<Node> cmp){
		Collections.sort(neighbors,cmp);
		neighbors.subList(this.firstLowNeighborIndex(), neighbors.size()).clear();
		neighbors.trimToSize();
	}

	private int firstLowNeighborIndex() {
		int i =0;
		Node neighbor;
		//TODO: use binary search on long lists of neighbors!!!
		while (i<neighbors.size()) {
			neighbor = neighbors.get(i);
			if (neighbor.getDegree() < degree || (neighbor.getDegree() == degree && neighbor.pos < this.pos)) {
				break;
			} else {
				i++;
			}
		}
		return i;

	}
}


class LPlusNNodesFilter {

	public void pruneNeighbors (Node node) {
		
		int i,j;
		i= 0;
		j= node.neighbors.size();
		Node neighbor;
		
		while (i < j){

			neighbor = node.neighbors.get(i);
			if((neighbor.degree < node.degree) || 
					(neighbor.degree == node.degree && neighbor.pos < node.pos)) {
				node.neighbors.set(i, node.neighbors.get(--j));
			} else {
				i++;
			}
		}
				
		node.neighbors.subList(i, node.neighbors.size()).clear();
		node.neighbors.trimToSize();
	}
}

/*
 *  We do not sort neighbors anymore, filtering is sufficient
 * 
 * class LPlusNNodesComparatorDecreasing implements Comparator<Node> {

	
	@Override
	public int compare(Node o1, Node o2) {
		int do1, do2;
		do1=o1.getDegree();
		do2 = o2.getDegree();

		if(do1!=do2) return (do2 - do1);
		else return o2.pos - o1.pos;

	}
}*/


