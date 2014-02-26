package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.AdjListGraph;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Round5Iterative extends Round5 {

	private final static String MULTI_OUT_NAME = "multiOutName";
	
	@Override
	public int run(String[] args) throws Exception {

		super.setup(args[0], args[1]);
		
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);
		
		MultipleOutputs.addNamedOutput(this.job, MULTI_OUT_NAME, TextOutputFormat.class,
				Text.class, Text.class);

		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private MultipleOutputs<Text, Text> mos;
		
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {


			//****** timing!
			CpuTimeLogger timeLog = new CpuTimeLogger("5 IR", key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));
			//ThreadMXBean thread = ManagementFactory.getThreadMXBean();
			//long cpu = thread.getCurrentThreadCpuTime();
			//****** 



			AdjListGraph g = new AdjListGraph();

			Iterator<Text> it = values.iterator()
					; 
			while (it.hasNext()){
				String next[]  = it.next().toString().split(QkCountDriver.NEIGHBORLIST_SEPARATOR);;

				//if(next.length!=2) throw new Error("Not an edge!");


				String a  = /*Checker.splitNodeAndDegree(*/next[0]/*)[0]*/;
				String b =  /*Checker.splitNodeAndDegree(*/next[1]/*)[0]*/;

				//Note: could we keep the graph oriented according to the small to large 
				//degree orientation of the whole graph?
				//How would node-iterator behave?
				//g.addOrientedEdge(a, b);

				g.addEdge(a,b);

				//Debug
				//output.collect(key, new Text("Edge " + a + " - " + b));

			}
			Configuration conf = context.getConfiguration();

			long graphThresholdSize = conf.getLong(QkCountDriver.ITERATE_OVER_SIZE_CONF_KEY, -1);

			if (graphThresholdSize==-1) {
				throw new Error("Graph threshold size undefined!");
			}

			if(g.getUnorientedSize()<graphThresholdSize) {

				//TODO Disallow to use both sampling and iteration (sampling is not applied if iteration is enabled)
				long count=g.countCliquesOfSize(context.getConfiguration().getInt(QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1)-1);

				context.write(key,new Text(Long.toString(count)));

			} else {

				//writing down the edge list of the subgraph for iteration
				String outDir= QkCountDriver.buildPath(conf,QkCountDriver.ITER_IN);
				
				
				
				String keyString = key.toString();
				ArrayList<String> nodes = g.getNodeList();  
				Iterator<String> nodeIterator = nodes.iterator();
				while (nodeIterator.hasNext()) {
					String node = nodeIterator.next();
					ArrayList<String> neighbors = g.getLargerNeighbors(node);
					Iterator<String> neighIterator = neighbors.iterator();
					while(neighIterator.hasNext()){
						String neighbor = neighIterator.next();
						mos.write(/*QkCountDriver.ITER_IN,*/ 
								new TextWithHashing(keyString + QkCountDriver.GRAPH_ID_SEPARATOR+node), 
								new Text(keyString + QkCountDriver.GRAPH_ID_SEPARATOR+neighbor),
								outDir
								);
						mos.write(/*QkCountDriver.ITER_IN,*/ 
								new TextWithHashing(keyString + QkCountDriver.GRAPH_ID_SEPARATOR+neighbor), 
								new Text(keyString + QkCountDriver.GRAPH_ID_SEPARATOR+node), 
								outDir
								);
					}
				}
			}

			//****** timing!
			timeLog.logCpuTime(Long.toString(g.getUnorientedSize()),Integer.toString(g.getNodesNumber()));
			//cpu = thread.getCurrentThreadCpuTime() - cpu;
			//context.write(new Text(key + " - Reduce5-CPU-Time"), new Text("" + cpu));
			//****** 

		}
	}
}
