package it.uniroma1.di.fff.QkCount;


import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;

public class Round3 extends AbstractRound implements Tool {	


	@Override
	public int run(String[] args) throws Exception {

		//this.jobName = "Round3";
		
		super.setup(args[0], args[1]);
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);


		return this.job.waitForCompletion(true) ? 0 : 1;


	}


	public static class Map extends Mapper<Text, Text, TextWithHashing, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			//output.collect(value, key);

			String[] srcPair, dstPair;

			srcPair = Checker.splitNodeAndDegree(key.toString());
			dstPair = Checker.splitNodeAndDegree(value.toString());

			String src,dst;
			int srcDegree,dstDegree;

			src = srcPair[0];
			dst = dstPair[0];

			srcDegree = Integer.parseInt(srcPair[1]);

			dstDegree = Integer.parseInt(dstPair[1]);

			if(Checker.DoubleCheck(src,srcDegree,dst,dstDegree))
				//output.collect(new Text(srcPair[0]+ Driver.NODE_DEGREE_SEPARATOR + srcPair[1]),
				//		 new Text(dstPair[0]+ Driver.NODE_DEGREE_SEPARATOR + dstPair[1]));
				context.write(new TextWithHashing(key.toString()),  value);
		}
	}

	public static class Reduce extends Reducer<TextWithHashing, Text, TextWithHashing, Text> {

		@Override
		public void reduce(TextWithHashing key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {


			CpuTimeLogger timeLog = new CpuTimeLogger("3 R", key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));

			StringBuffer sb = new StringBuffer();
			int size = 0;

			Iterator<Text> it = values.iterator();
			while (it.hasNext()){
				Text next = it.next();
				size++;
				sb.append(next);
				sb.append(QkCountDriver.NEIGHBORLIST_SEPARATOR);
			}
			sb.setLength(sb.length() - QkCountDriver.NEIGHBORLIST_SEPARATOR.length());

			int cliqueSize = context.getConfiguration().getInt(QkCountDriver.CLIQUE_SIZE_CONF_KEY, -1);
			if (size >=  cliqueSize-1) {
				context.write(new TextWithHashing(key.toString()),new Text(sb.toString()));
			}
			
			timeLog.logCpuTime(Integer.toString(size), size>=cliqueSize-1?"1":"0");
		}
	}
}
