package it.uniroma1.di.fff.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;


public  class LPlusNGraphWithBuckets extends LPlusNGraph {

	int nBuckets;
	int [] partition;

	public LPlusNGraphWithBuckets (int nBuckets , int [] partition) {

		super();
		this.nBuckets = nBuckets;
		this.partition = partition;

	} 


	@Override
	protected void prepareForCounting() {

		LPlusNNodesWithBucketsFilter filter = new LPlusNNodesWithBucketsFilter(/*nBuckets, partition*/);
		LPlusNNodesWithBucketsComparatorIncreasing cmp = new LPlusNNodesWithBucketsComparatorIncreasing();
		for (Node n : nodes) {
			filter.pruneNeighbors((NodeWithBuckets)n);
			Collections.sort(n.neighbors, cmp);
		}
	}

	@Override
	public long countCliquesOfSize(int cliqueSize) {

		//DEBUG
		/*StringBuffer sbPartition = new StringBuffer();
		
		for (int i = 0; i<partition.length; i++) {
			sbPartition.append(partition[i] + QkCountDriver.NEIGHBORLIST_SEPARATOR);
		}
		sbPartition.setLength(sbPartition.length() -QkCountDriver.NEIGHBORLIST_SEPARATOR.length());
		
		
		FileWriter bw;
		PrintWriter dbg =null;

		try {
			bw = new FileWriter("datasets/cliqueList.txt",true);
			dbg  = new PrintWriter(bw);
		} catch (Exception e) {}
		String[] currentClique = new String[cliqueSize];*/
		//DEBUG


		this.prepareForCounting();

		long res = 0;

		nodeMapper = null;


		ArrayList<Iterator<Node>> queues = new ArrayList<Iterator<Node>>(cliqueSize -2); 
		ArrayList<ArrayList<Node>> marked = new ArrayList<ArrayList<Node>>(cliqueSize -2);
		ArrayList<Node> list;
		ArrayList<Node> markedList;

		for(int i = 0; i < cliqueSize-2; i++) {
			marked.add(i, null);
			queues.add(i, null);
		}


		byte mark,currentQueue;
		for (Node n : nodes) {

			if(((NodeWithBuckets)n).bucket == partition[0]) {

				mark = 1;
				currentQueue = 0;

				Node currentNode = n;

				//DEBUG
				//currentClique[currentQueue] = ((NodeWithBuckets)currentNode).realLabel;
				//DEBUG

				list = new ArrayList<Node>(n.getGammaPlus().size());
				markedList = new ArrayList<Node>(n.getGammaPlus().size());

				for (Node i : n.getGammaPlus()) {
					i.setMark(mark);
					markedList.add(i);
					if(((NodeWithBuckets)i).bucket ==partition[mark]) {
						list.add(i);
					}

				}

				queues.set(currentQueue, list.iterator());
				marked.set(currentQueue, markedList);


				boolean foundEmpty = false;

				while (currentQueue >=0 && (foundEmpty || queues.get(currentQueue).hasNext())) {

					if (!foundEmpty){
						currentNode = queues.get(currentQueue).next();

						//DEBUG
						/*if(currentNode!= null) {
							currentClique[currentQueue+1] = ((NodeWithBuckets)currentNode).realLabel;
						}*/
						//DEBUG
					}


					//ADDING NODES TO THE QUEUES AND MARKING
					if(mark<cliqueSize -2 && !foundEmpty) {

						mark ++;
						currentQueue ++;

						list = new ArrayList<Node>();
						markedList = new ArrayList<Node>();

						marked.set(currentQueue, markedList);

						for (Node i : currentNode.getGammaPlus()) {


							if (i.getMark()==mark-1) {
								i.setMark(mark);
								markedList.add(i);
								if(((NodeWithBuckets)i).bucket == partition[mark]) {
									list.add(i);
								}
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

								if (i.getMark()==mark && (((NodeWithBuckets)i).bucket == partition[mark+1])) {
									res++;
									
									//DEBUG
									/*currentClique[cliqueSize-1] = ((NodeWithBuckets)i).realLabel;
									
									String [] CCCopy = new String[cliqueSize];
									for (int z = 0; z<cliqueSize; z++) {
										CCCopy[z]=currentClique[z];
									}
									
									Arrays.sort(CCCopy);
									
									StringBuffer sb = new StringBuffer();
									for (int z = 0; z < cliqueSize; z++) {
										sb.append(CCCopy[z] + QkCountDriver.NEIGHBORLIST_SEPARATOR);
									}
									sb.setLength(sb.length() - QkCountDriver.NEIGHBORLIST_SEPARATOR.length());
									
									dbg.println(sb.toString()+ " - " + sbPartition.toString());*/
									//DEBUG 

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

		}

		//DEBUG
		/*try {
			dbg.close();
		} catch (Exception e) {}*/
		//DEBUG 


		return res;
	}


	/*@Override
	public void addEdge(String a, String b) {
		super.addEdge(a, b);
	}*/


	@Override
	protected Node getNode(String nodeLabel) {

		Integer pos = nodeMapper.get(nodeLabel);
		Node n;

		if(pos==null) {
			pos = new Integer(nNodes);
			n = new NodeWithBuckets(nNodes, Math.abs(nodeLabel.hashCode()) % nBuckets /*, nodeLabel*/);
			nodeMapper.put(nodeLabel, pos);
			nodes.add(n); 
			nNodes ++;
		} else {
			n = nodes.get(pos);
		}
		return n;
	}

}


class NodeWithBuckets extends Node {

	/*String realLabel;*/
	protected int bucket;

	protected NodeWithBuckets (int pos, int bucket/*, String realLabel*/) {
		super(pos);

		//this.realLabel = realLabel;
		this.bucket = bucket;

	}

}

class LPlusNNodesWithBucketsFilter {

	/*int nBuckets;
	int [] partition;
	int [] bucketToFirstPartition;


	LPlusNNodesFilter (int nBuckets, int [] partition) {
		this.nBuckets= nBuckets;
		this.partition = partition;


		bucketToFirstPartition = new int [nBuckets];

		for (int i = partition.length-1; i>=0;  i--){
			bucketToFirstPartition[partition[i]] = i;
		}
	}*/

	public void pruneNeighbors (NodeWithBuckets node) {

		int i,j;
		i= 0;
		j= node.neighbors.size();
		NodeWithBuckets neighbor;


		while (i < j){

			neighbor = (NodeWithBuckets)node.neighbors.get(i);
			if((neighbor.bucket < node.bucket) || 
					(neighbor.bucket == node.bucket && neighbor.pos < node.pos)) {
				node.neighbors.set(i, node.neighbors.get(--j));
			} else {
				i++;
			}
		}



		node.neighbors.subList(i, node.neighbors.size()).clear();
		node.neighbors.trimToSize();
	}

	/*public void pruneNeighbors (NodeWithBuckets node) {

		ArrayList<Node> neighbors = node.neighbors;

		int i,j;
		i= 0;
		j = neighbors.size();

		int minToKeep;

		if(bucketToFirstPartition[node.bucket]+1 < partition.length) {
			minToKeep = partition[bucketToFirstPartition[node.bucket]+1]; 
		} else {
			minToKeep = node.bucket;
		}



		NodeWithBuckets neighbor;

		while (i < j){

			neighbor = (NodeWithBuckets)neighbors.get(i);
			if((neighbor.bucket < minToKeep) || 
					(neighbor.bucket == node.bucket && neighbor.pos < node.pos)) {
				neighbors.set(i, neighbors.get(--j));
			} else {
				i++;
			}

		}
		node.neighbors.subList(i, neighbors.size()).clear();
		node.neighbors.trimToSize();

	}*/
}

class LPlusNNodesWithBucketsComparatorIncreasing implements Comparator<Node> {


	@Override
	public int compare(Node o1, Node o2) {
		NodeWithBuckets n1, n2;

		n1 = (NodeWithBuckets)o1;
		n2 = (NodeWithBuckets)o2;

		if(n1.bucket != n2.bucket) {
			return n1.bucket-n2.bucket;
		} else {
			return n1.pos-n2.pos;
		}

	}
}


