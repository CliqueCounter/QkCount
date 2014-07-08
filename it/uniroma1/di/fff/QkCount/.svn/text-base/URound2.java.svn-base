package it.uniroma1.di.fff.QkCount;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class URound2 extends AbstractRound implements Tool {	

	@Override
	public int run(String[] args) throws Exception {	
				
		super.setup(args);
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);
		return this.job.waitForCompletion(true) ? 0 : 1;
		
	}

	public static class Map extends Mapper<Text,Text,Text,Text> {

		@Override
		public void map(Text key,Text value,Context context) throws IOException, InterruptedException {
		
			
				context.write(key, value);
				
		}
			
	}

	public static class Reduce extends Reducer <Text,Text,Text,LongWritable> {

		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			
			Iterator<Text> itr = values.iterator();
			long n_K4 = 0;
			
			while(itr.hasNext()) {
				
				n_K4 += Long.parseLong(itr.next().toString());
				
			}
			
			context.write(key,new LongWritable(n_K4));
			
		}
		
	}
	
}