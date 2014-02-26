package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.TextWithHashing;

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
				
		//this.jobName = "Round" + this.conf.getInt(QkCountDriver.ROUND_NUMBER_CONF_KEY,-1);
		
		super.setup(args[0], args[1]);
	
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);

		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	/*public static class Map1 extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

		 @Override
		 public void map(Text key,Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			 output.collect(key, value);
		 }
	 }*/
	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			//Inverting key-value pairs and filtering out self-loops
			if(!key.equals(value)) {
				context.write(new TextWithHashing(value.toString()), key);
			}
		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, Text> {

		@Override
		//@CpuTimeLogable
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			//****** timing!
			CpuTimeLogger timeLog = new CpuTimeLogger( 
					context.getConfiguration().getInt(QkCountDriver.ROUND_NUMBER_CONF_KEY, -1) 
					+ " R",key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true)
					);			 
			//ThreadMXBean thread = ManagementFactory.getThreadMXBean();
			//long cpu = thread.getCurrentThreadCpuTime();
			//****** 			 
			ArrayList<String> storage = new ArrayList<String>();
			//int key_degree = 0;

			Iterator<Text> it = values.iterator();
			while(it.hasNext())
				storage.add((it.next()).toString());

			//key_degree = storage.size();

			Text newKey=new TextWithHashing(key+ QkCountDriver.NODE_DEGREE_SEPARATOR +storage.size());
			
			for(int i=0;i<storage.size();i++)
				context.write(newKey, new Text(storage.get(i)));

			//****** timing!
			String inputOutputSizes =Integer.toString(storage.size()); 
			timeLog.logCpuTime(inputOutputSizes,inputOutputSizes );
			//cpu = thread.getCurrentThreadCpuTime() - cpu;
			//context.write(new Text(key + " - Reduce5-CPU-Time"), new Text("" + cpu));
			//****** 
		}
	}
}
