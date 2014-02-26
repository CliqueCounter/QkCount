package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;



public class Round6 extends AbstractRound implements Tool {	


	@Override
	public int run(String[] args) throws Exception {
		//this.jobName="Round6";

		super.setup(args[0], args[1]);

		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);


		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			context.write(new TextWithHashing("TotCliques"), value);

		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, Text> {

		@Override
		//@CpuTimeLogable
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			//****** timing!
			//CpuTimeLogger timeLog = new CpuTimeLogger("6 R", key.toString());			 
			//****** 			 
			long totCliques= 0;

			Iterator<Text> it = values.iterator();
			while(it.hasNext())
				totCliques+= Long.parseLong(it.next().toString());
			
			
			double samplingProbability = context.getConfiguration().getDouble(
					QkCountDriver.EDGE_SAMPLING_PROBABILITY_CONF_KEY, -1);
			int nColors = context.getConfiguration().getInt(
					QkCountDriver.COLOR_SAMPLING_COLORS_CONF_KEY, -1);
			int cliqueSize = context.getConfiguration().getInt(
					QkCountDriver.CLIQUE_SIZE_CONF_KEY,-1);
			
			String outKey; 
			if(samplingProbability > 0) {
				//totCliques = Math.round((double)totCliques/Math.pow(samplingProbability, ((cliqueSize-1) * (cliqueSize-2))/2));
				outKey = "E " +samplingProbability + " " + cliqueSize; 
			} else {
				if(nColors>0) {
					//totCliques= Math.round(totCliques*(Math.pow(nColors, cliqueSize-2)));
					outKey = "C "+ nColors + " " + cliqueSize;
				} else {
					outKey = "X "+ "- " + cliqueSize;
				}
			}
			
			//Long partialCount = context.getConfiguration().getLong(QkCountDriver.PARTIAL_COUNT_BEFORE_ITERATION_CONF_KEY, 0);
			//context.getConfiguration().setLong(QkCountDriver.PARTIAL_COUNT_BEFORE_ITERATION_CONF_KEY,totCliques);
			context.write(new TextWithHashing(outKey), new Text(Long.toString(totCliques/*+partialCount*/)));

			//****** timing!
			//timeLog.logCpuTime();
			//timeLog.logCpuTime();
			//****** 
		}
	}
}
