package it.uniroma1.di.fff.QkCount;


import it.uniroma1.di.fff.Util.AdjListGraph;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.TextWithHashing;

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
		//this.jobName="Round5";
		
		super.setup(args[0], args[1]);
		
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);
			
		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, TextWithHashing, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			CpuTimeLogger timeLog = new CpuTimeLogger("5 M", key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));
			
			long neighListSize = 0;
			
			StringTokenizer it = new StringTokenizer(value.toString(),QkCountDriver.NEIGHBORLIST_SEPARATOR); 
			while(it.hasMoreElements()) {
				neighListSize++;
				String element = it.nextToken();
				context.write(new TextWithHashing(element), key);
			}
			
			timeLog.logCpuTime(Long.toString(neighListSize),Long.toString(neighListSize));

		}
	}
	
	

	public static class Reduce extends Reducer<Text, Text, TextWithHashing, Text> {
		
		@Override
		public void reduce(Text key,
				 Iterable<Text> values,
				 Context context) throws IOException, InterruptedException {

			
			//****** timing!
			CpuTimeLogger timeLog = new CpuTimeLogger("5 R", key.toString(),
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
			
			long count=g.countCliquesOfSize(context.getConfiguration().getInt(QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1)-1);
			
			//boolean isSampled = Boolean.parseBoolean(key.toString().split(QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR)[1]);
			 
			
				Configuration conf = context.getConfiguration();
				double samplingProbability = conf.getDouble(
						QkCountDriver.EDGE_SAMPLING_PROBABILITY_CONF_KEY, -1);
				int nColors = conf.getInt(
						QkCountDriver.COLOR_SAMPLING_COLORS_CONF_KEY, -1);
				int cliqueSize = conf.getInt(
						QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1);
				
				if(samplingProbability > 0) {
					samplingProbability = Double.parseDouble(key.toString().split(QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR)[1]);
					count = Math.round((double)count/Math.pow(samplingProbability, ((cliqueSize-1) * (cliqueSize-2))/2)); 
				} else {
					if(nColors>0) {
						//try {
						nColors=Integer.parseInt(key.toString().split(QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR)[1]);
						count= Math.round(count*(Math.pow(nColors, cliqueSize-2)));
						/*} catch (Exception e) {
							System.out.println(key.toString());
							throw(e);
						}*/
					}  
				}
			
			//} else {
			context.write(new TextWithHashing(key.toString()),new Text(Long.toString(count)));
			//}

			//****** timing!
			timeLog.logCpuTime(Long.toString(g.getUnorientedSize()),Integer.toString(g.getNodesNumber()));
            //cpu = thread.getCurrentThreadCpuTime() - cpu;
            //context.write(new Text(key + " - Reduce5-CPU-Time"), new Text("" + cpu));
			//****** 

		}
	}
}
