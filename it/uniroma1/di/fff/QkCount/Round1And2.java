/* ============================================================================
 *  Round1And2.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Map and reduce functions to associate each edge endpoint
 *  						with the degree of the corresponding node. (in two rounds)
 *  					
 */

package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;



public class Round1And2 extends AbstractRound implements Tool {	


	@Override
	public int run(String[] args) throws Exception {	


		super.setup(args[0], args[1]);

		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);

		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		int ROUND_NUMBER;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			this.ROUND_NUMBER = context.getConfiguration().getInt(QkCountDriver.ROUND_NUMBER_CONF_KEY, -1);

		}

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			if(!key.equals(value)) {
				context.write(value, key);
				//if(!key.toString().contains(QkCountDriver.NODE_DEGREE_SEPARATOR)) {
				if(this.ROUND_NUMBER==1) {
					//Input files must have each edge in only one direction
					// here we reconstruct the edge in the opposite direction
					context.write(key, value);
				}
			}
		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, Text> {

		int ROUND_NUMBER;
		boolean LOG_CPU_TIME;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			this.ROUND_NUMBER = context.getConfiguration().getInt(QkCountDriver.ROUND_NUMBER_CONF_KEY, -1);
			this.LOG_CPU_TIME = context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true);
		}

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			//****** timing!
			CpuTimeLogger timeLog = new CpuTimeLogger( 
					ROUND_NUMBER 
					+ " R",key.toString(),
					LOG_CPU_TIME
					);			 
			//****** 			 
			ArrayList<String> storage = new ArrayList<String>();

			Iterator<Text> it = values.iterator();
			while(it.hasNext())
				storage.add((it.next()).toString());


			String keyStr=key.toString();
			int keyDegree = storage.size();
			Text newKey=new Text(keyStr + QkCountDriver.NODE_DEGREE_SEPARATOR +keyDegree);

			if(this.ROUND_NUMBER == 2) {
				String [] neighAndDegree;
				for(int i=0;i<storage.size();i++) {
					neighAndDegree = Checker.splitNodeAndDegree(storage.get(i));
					if (Checker.DoubleCheck(keyStr, keyDegree, neighAndDegree[0], Integer.parseInt(neighAndDegree[1]))) {
						context.write(newKey, new Text(storage.get(i)));
					}
				}
			} else {
				for(int i=0;i<storage.size();i++) {
					context.write(newKey, new Text(storage.get(i)));
				}
			}

			//****** timing!
			String inputOutputSizes =Integer.toString(storage.size()); 
			timeLog.logCpuTime(inputOutputSizes,inputOutputSizes );
			//****** 
		}
	}
}
