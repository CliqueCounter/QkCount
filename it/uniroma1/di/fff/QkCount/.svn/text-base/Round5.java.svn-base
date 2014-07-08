/* ============================================================================
 *  Round5.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		construction of G+(u) and actual counting of the k-cliques
 *  					
 */


package it.uniroma1.di.fff.QkCount;


import it.uniroma1.di.fff.Util.AdjListGraph;
import it.uniroma1.di.fff.Util.CliqueCounterGraph;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.LPlusNGraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;


public class Round5 extends AbstractRound implements Tool {	

	@Override
	public int run(String[] args) throws Exception {


		super.setup(args[0], args[1]);

		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);

		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		boolean LOG_CPU_TIME;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			this.LOG_CPU_TIME = context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true);
		}

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			CpuTimeLogger timeLog = new CpuTimeLogger("5 M", key.toString(),
					this.LOG_CPU_TIME);

			long neighListSize = 0;

			StringTokenizer it = new StringTokenizer(value.toString(),QkCountDriver.NEIGHBORLIST_SEPARATOR); 
			while(it.hasMoreElements()) {
				neighListSize++;
				String element = it.nextToken();
				context.write(new Text(element), key);
			}

			timeLog.logCpuTime(Long.toString(neighListSize),Long.toString(neighListSize));

		}
	}



	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		//int numKeys =0 ; OBSOLETE?
		//long graphSizesSum = 0; OBSOLETE?

		int CLIQUE_SIZE;
		boolean USE_L_PLUS_N;
		boolean LOG_CPU_TIME;

		double SAMPLING_PROBABILITY;
		int N_COLORS;


		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();
			this.CLIQUE_SIZE = conf.getInt(QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1);
			this.USE_L_PLUS_N =  conf.getBoolean(QkCountDriver.USE_L_PLUS_N_CONF_KEY, true);
			this.LOG_CPU_TIME = conf.getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true);
			this.SAMPLING_PROBABILITY = conf.getDouble(QkCountDriver.EDGE_SAMPLING_PROBABILITY_CONF_KEY, -1);
			this.N_COLORS = conf.getInt(QkCountDriver.COLOR_SAMPLING_COLORS_CONF_KEY, -1);

		}

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {


			//****** timing!
			CpuTimeLogger timeLog = new CpuTimeLogger("5 R", key.toString(),
					LOG_CPU_TIME);
			//****** 



			CliqueCounterGraph g;
			if(this.USE_L_PLUS_N) {
				g= new LPlusNGraph();
			} else {
				g = new AdjListGraph();
			}
			int graphEdges = 0;

			Iterator<Text> it = values.iterator(); 
			while (it.hasNext()){
				String next[]  = it.next().toString().split(QkCountDriver.NEIGHBORLIST_SEPARATOR);;

				String a  = next[0];
				String b =  next[1];

				g.addEdge(a,b);
				graphEdges++;
			}



			long count=g.countCliquesOfSize(this.CLIQUE_SIZE-1);


			if(SAMPLING_PROBABILITY > 0) {
				SAMPLING_PROBABILITY = Double.parseDouble(key.toString().split(QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR)[1]);
				count = Math.round((double)count/Math.pow(SAMPLING_PROBABILITY, ((CLIQUE_SIZE-1) * (CLIQUE_SIZE-2))/2)); 
			} else {
				if(N_COLORS>0) {

					N_COLORS=Integer.parseInt(key.toString().split(QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR)[1]);
					count= Math.round(count*(Math.pow(N_COLORS, CLIQUE_SIZE-2)));

				}  
			}

			context.write(new Text(key.toString()),new Text(Long.toString(count)));


			//****** timing!
			timeLog.logCpuTime(Long.toString(graphEdges),Integer.toString(g.getNodesNumber()));
			//numKeys ++;
			//graphSizesSum += g.getUnorientedSize();
			//****** 

		}
	}
}
