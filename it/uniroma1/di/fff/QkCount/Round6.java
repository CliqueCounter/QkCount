/* ============================================================================
 *  Round6.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		summing up partial counts
 *  					
*/package it.uniroma1.di.fff.QkCount;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;



public class Round6 extends AbstractRound implements Tool {	


	@Override
	public int run(String[] args) throws Exception {

		super.setup(args[0], args[1]);

		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);


		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text ("TotCliques"), value);

		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, Text> {

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

			long totCliques= 0;

			Iterator<Text> it = values.iterator();
			while(it.hasNext())
				totCliques+= Long.parseLong(it.next().toString());
			
			
			String outKey; 
			if(SAMPLING_PROBABILITY > 0) {

				outKey = "E " +SAMPLING_PROBABILITY + " " + CLIQUE_SIZE; 
			} else {
				if(N_COLORS>0) {

					outKey = "C "+ N_COLORS + " " + CLIQUE_SIZE;
				} else {
					outKey = "X "+ "- " + CLIQUE_SIZE;
				}
			}
			
			context.write(new Text (outKey), new Text(Long.toString(totCliques/*+partialCount*/)));

		}
	}
}
