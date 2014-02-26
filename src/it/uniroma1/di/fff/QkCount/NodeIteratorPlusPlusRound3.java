package it.uniroma1.di.fff.QkCount;


import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.DoubleChecker;
import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;

public class NodeIteratorPlusPlusRound3 extends AbstractRound implements Tool {	


	@Override
	public int run(String[] args) throws Exception {

		//this.jobName = "Round3";
		
		super.setup(args[0], args[1]);		
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);


		return this.job.waitForCompletion(true) ? 0 : 1;


	}


	public static class Map extends Mapper<Text, Text, Text, Text> {

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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {


			CpuTimeLogger timeLog = new CpuTimeLogger("3 R", key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));

			ArrayList<String> l = new ArrayList<String>();

			Iterator<Text> it = values.iterator();
			while (it.hasNext()){
				Text next = it.next();
				l.add(next.toString());
			}
			

			
			if (l.size() >=  2) {
				Collections.sort(l,new DoubleChecker());
				for (int i = 0; i < l.size(); i++) {
					for (int j= i+1; j<l.size(); j++){
						context.write(new TextWithHashing(Checker.splitNodeAndDegree(key.toString())[0]),
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
