package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;



public class NodeIteratorPlusPlusRound5 extends AbstractRound implements Tool {	


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
			long totTriangles= 0;

			Iterator<Text> it = values.iterator();
			while(it.hasNext())
				totTriangles+= Long.parseLong(it.next().toString());
			
			
			context.write(new TextWithHashing("NI - 3"), new Text(Long.toString(totTriangles)));

			//****** timing!
			//timeLog.logCpuTime();
			//timeLog.logCpuTime();
			//****** 
		}
	}
}
