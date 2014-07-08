/* ============================================================================
 *  URound1.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Clique counter based on 
 *  					Foto N. Afrati, Dimitris Fotakis, and Jeffrey D. Ullman
 *  					Enumerating subgraph instances using map-reduce
 *  					Proceedings of the 29th IEEE International Conference on Data Engineering ICDE 2013
 *  
 */


package it.uniroma1.di.fff.QkCount;
import it.uniroma1.di.fff.Util.LPlusNGraphWithBuckets;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;

public class URound1 extends AbstractRound implements Tool {

	private static void initializePartition(int[] A,int hash_src,int hash_dst,int srcIndex,int dstIndex,int B) {

		if((hash_src >= hash_dst && srcIndex != dstIndex-1) || (hash_dst >= B-1 && dstIndex < A.length-1)) { 
			//Throw error?
			throw new Error("Wrong partition init!");
		}
		
		A[srcIndex] = hash_src;
		A[dstIndex] = hash_dst;
		
		for (int i=0;i<srcIndex;i++)
			A[i] = 0;
		
		for (int i=srcIndex+1;i<dstIndex;i++)
			A[i] = hash_src+1;

		for (int i=dstIndex+1;i<A.length;i++)
			A[i] = hash_dst+1;

		/*if((hash_src == hash_dst) && (end != 1) && (hash_src == 0 || start == 0))
			return URound1.IsCompleteGenerationStep(A,hash_src,hash_dst,start,end,B,false);*/

		

	}

	private static void UpdatePartition(int[] A,int val,int start,int end) {

		for (int i=start;i<=end;i++)
			A[i] = val;
	}

	private static boolean hasMorePartitions(int[] A,int hash_src,int hash_dst,int srcIndex,int dstIndex,int B) {

		

		for(int i=A.length-1;i>=0;i--) {

			if(i>dstIndex) {

				if(A[i]<B-1) {

					A[i] = A[i] + 1;

					if (A[i]!=B-1 && i!=A.length-1)
						URound1.UpdatePartition(A,A[i],i+1,A.length-1);

					return true;

				}

			}

			else if(i<dstIndex && i>srcIndex) {

				if(A[i]<hash_dst) {

					A[i] = A[i] + 1;

					if (A[i] != hash_dst) {

						URound1.UpdatePartition(A,A[i],i+1,dstIndex-1);
					}
					URound1.UpdatePartition(A,A[dstIndex]+1,dstIndex+1,A.length-1);

					return true;

				}

			}

			else {

				if(i<srcIndex && A[i]<hash_src) {

					A[i] = A[i] + 1;

					if (A[i]!=hash_src) {

						URound1.UpdatePartition(A,A[i],i+1,srcIndex-1);
					}
					URound1.UpdatePartition(A,A[srcIndex]+1,srcIndex+1,dstIndex-1);
					URound1.UpdatePartition(A,A[dstIndex]+1,dstIndex+1,A.length-1);

					return true;

				}

			}

		}

		return false;

	}

	@Override
	public int run(String[] args) throws Exception {	

		super.setup(args);
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);
		return this.job.waitForCompletion(true) ? 0 : 1;

	}

	public static class Map extends Mapper<Text,Text,Text,Text> {

		int N_BUCKETS;
		int CLIQUE_SIZE;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);
			this.N_BUCKETS = context.getConfiguration().getInt(QkCountDriver.NUMBER_OF_BUCKETS_CONF_KEY,-1);
			this.CLIQUE_SIZE = context.getConfiguration().getInt(QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1);

		}
		
		

		@Override
		public void map(Text key,Text value,Context context) throws IOException, InterruptedException {

			int tmp;
			String strTmp,strKey,strValue;

			strKey = key.toString();
			strValue = value.toString();
			int keyBucket = Math.abs(strKey.hashCode()) % N_BUCKETS;
			int valueBucket = Math.abs(strValue.hashCode()) % N_BUCKETS;

			if(keyBucket > valueBucket) {

				tmp = keyBucket;
				keyBucket = valueBucket;
				valueBucket = tmp;

				strTmp = strValue;
				strValue = strKey;
				strKey = strTmp;

			}

			int[] generatePartitions = new int[CLIQUE_SIZE];
			StringBuffer partition;
			Integer elem;

			int jMin;
			int jTop;
			
			int iMin = keyBucket < N_BUCKETS-1 ? 0 : CLIQUE_SIZE-2;
			
			for (int i=iMin;i<(CLIQUE_SIZE)-1;i++) {
				
				jMin = valueBucket < N_BUCKETS -1 ? i+1: CLIQUE_SIZE -1;
				jTop = keyBucket < valueBucket ? CLIQUE_SIZE : i+2;
				
				for (int j=jMin; j<jTop; j++) {
					
					URound1.initializePartition(generatePartitions, keyBucket, valueBucket, i, j, N_BUCKETS);
					
					do {
						partition = new StringBuffer();

						for (int k=0;k<CLIQUE_SIZE;k++) {

							elem = new Integer(generatePartitions[k]);
							partition.append(elem.toString());

							partition.append(QkCountDriver.PARTITIONS_SEPARATOR);

						}
						
						partition.setLength(partition.length() - QkCountDriver.PARTITIONS_SEPARATOR.length());

						context.write(new Text(partition.toString()),new Text(strKey+QkCountDriver.NEIGHBORLIST_SEPARATOR+strValue));
						
					} while (URound1.hasMorePartitions(generatePartitions,keyBucket,valueBucket,i,j,N_BUCKETS));


				}

			}

		}
		
		
		//Debug
		public void dbgSetup (int nBuckets, int cliqueSize) {
			this.N_BUCKETS = nBuckets;
			this.CLIQUE_SIZE = cliqueSize;
		}
		//Debug
		public void testMap (Text key,Text value){
			int tmp;
			String strTmp,strKey,strValue;

			strKey = key.toString();
			strValue = value.toString();
			int hash_key = Math.abs(strKey.hashCode()) % N_BUCKETS;
			int hash_value = Math.abs(strValue.hashCode()) % N_BUCKETS;

			if(hash_key > hash_value) {

				tmp = hash_key;
				hash_key = hash_value;
				hash_value = tmp;

				strTmp = strValue;
				strValue = strKey;
				strKey = strTmp;

			}

			int[] generatePartitions = new int[CLIQUE_SIZE];
			StringBuffer partition;
			Integer elem;

			int jMin;
			int jTop;
			
			int iMin = hash_key < N_BUCKETS-1 ? 0 : CLIQUE_SIZE-2;
			
			for (int i=iMin;i<(CLIQUE_SIZE)-1;i++) {
				
				jMin = hash_value < N_BUCKETS -1 ? i+1: CLIQUE_SIZE -1;
				jTop = hash_key < hash_value ? CLIQUE_SIZE : i+2;
				
				for (int j=jMin; j<jTop; j++) {
					
					URound1.initializePartition(generatePartitions, hash_key, hash_value, i, j, N_BUCKETS);
					
					do {
						partition = new StringBuffer();

						for (int k=0;k<CLIQUE_SIZE;k++) {

							elem = new Integer(generatePartitions[k]);
							partition.append(elem.toString());

							partition.append(QkCountDriver.PARTITIONS_SEPARATOR);

						}
						
						partition.setLength(partition.length() - QkCountDriver.PARTITIONS_SEPARATOR.length());

						System.out.println(partition.toString() + " - " + strKey+QkCountDriver.NEIGHBORLIST_SEPARATOR+strValue + " (" + hash_key +","+hash_value+")" );
						//context.write(new Text(partition.toString()),new Text(strKey+QkCountDriver.NEIGHBORLIST_SEPARATOR+strValue));
						
					} while (URound1.hasMorePartitions(generatePartitions,hash_key,hash_value,i,j,N_BUCKETS));


				}

			}
		}

	}
	

	public static class Reduce extends Reducer <Text,Text,Text,Text> {

		int N_BUCKETS;
		int CLIQUE_SIZE;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {

			super.setup(context);
			this.N_BUCKETS = context.getConfiguration().getInt(QkCountDriver.NUMBER_OF_BUCKETS_CONF_KEY,-1);
			this.CLIQUE_SIZE = context.getConfiguration().getInt(QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1);

		}

		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {

			String[] partition = key.toString().split(Pattern.quote(QkCountDriver.PARTITIONS_SEPARATOR));
			int [] partNums = new int[this.CLIQUE_SIZE];


			for (int i = 0; i<partition.length; i++)
				partNums[i] = Integer.parseInt(partition[i]);

			LPlusNGraphWithBuckets graph = new LPlusNGraphWithBuckets(this.N_BUCKETS,partNums);

			for (Text t : values) {

				String[] nodes = t.toString().split(Pattern.quote(QkCountDriver.NEIGHBORLIST_SEPARATOR));
				graph.addEdge(nodes[0],nodes[1]);

			}

			long cliques = graph.countCliquesOfSize(this.CLIQUE_SIZE);
			context.write(new Text("U - " + this.CLIQUE_SIZE),new Text(Long.toString(cliques)));

		}

	}

}