/* ============================================================================
 *  NodeIteratorPlusPlusRound3.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Triangle counter based on 
 *  						Suri, Siddharth, and Sergei Vassilvitskii. 
 *  						"Counting triangles and the curse of the last reducer." 
 *  						Proceedings of the 20th international conference on World wide web.
 *  						ACM, 2011.
 *  					
*/

package it.uniroma1.di.fff.QkCount;


import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.DoubleChecker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;

public class NodeIteratorPlusPlusRound3 extends AbstractRound implements Tool {	

	boolean useLazyPairs;
	
	public NodeIteratorPlusPlusRound3(boolean useLazyPairs) {
		super();
		this.useLazyPairs = useLazyPairs;
	}

	@Override
	public int run(String[] args) throws Exception {

		//this.jobName = "Round3";
		
		super.setup(args[0], args[1]);		
		this.job.setMapperClass(Map.class);
		if(useLazyPairs) {
			this.job.setReducerClass(LazyPairRedeuce.class);
		} else {
			this.job.setReducerClass(Reduce.class);
		}


		return this.job.waitForCompletion(true) ? 0 : 1;


	}


	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			context.write(key, value);
		}
	}

	public static class LazyPairRedeuce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			StringBuffer sb = new StringBuffer();
			
			for (Text t : values) {
				sb.append(t.toString());
				sb.append(QkCountDriver.NEIGHBORLIST_SEPARATOR);
			}
			sb.setLength(sb.length()-QkCountDriver.NEIGHBORLIST_SEPARATOR.length());
			
			context.write(key, new Text(sb.toString()));
		}
		
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		boolean LOG_CPU_TIME;

		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			LOG_CPU_TIME = context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true);
		}

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {


			CpuTimeLogger timeLog = new CpuTimeLogger("3 R", key.toString(),
					LOG_CPU_TIME);

			ArrayList<String> l = new ArrayList<String>();

			Iterator<Text> it = values.iterator();
			while (it.hasNext()){
				Text next = it.next();
				l.add(next.toString());
			}
			

			Text newKey = new Text(Checker.splitNodeAndDegree(key.toString())[0]);
			
			if (l.size() >=  2) {
				Collections.sort(l,new DoubleChecker());
				for (int i = 0; i < l.size(); i++) {
					for (int j= i+1; j<l.size(); j++){
						context.write(newKey,
								new Text(
										Checker.splitNodeAndDegree(l.get(i))[0]+QkCountDriver.NEIGHBORLIST_SEPARATOR+
										Checker.splitNodeAndDegree(l.get(j))[0]
										));
					}
				}
			}
			
			timeLog.logCpuTime(Integer.toString(l.size()), l.size()>=2?"1":"0");
		}
	}
}
