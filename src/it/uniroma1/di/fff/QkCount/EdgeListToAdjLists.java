package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;



public class EdgeListToAdjLists extends AbstractRound implements Tool {	


	private static final String NEIGH_LIST_SEPARATOR = " ";
	private static final String LINE_PREAMBLE = "";
	private static final String LINE_END = ";";
	public static final String NODE_NEIGHBORS_SEPARATOR = " : ";
	
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
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			StringBuffer adjList = new StringBuffer();
			Iterator<Text> it = values.iterator();
			while(it.hasNext()) {
				adjList.append((it.next()).toString()+NEIGH_LIST_SEPARATOR);
				
			}
			if(adjList.length()>0) {
				adjList.setLength(adjList.length()-NEIGH_LIST_SEPARATOR.length());
				context.write(new TextWithHashing(LINE_PREAMBLE +key.toString()), new Text(adjList.toString()+LINE_END));

			}
			
		}
	}
}
