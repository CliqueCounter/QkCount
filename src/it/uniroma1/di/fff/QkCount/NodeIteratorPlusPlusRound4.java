package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;


public class NodeIteratorPlusPlusRound4 extends AbstractRound implements Tool {	

	@Override
	public int run(String[] args) throws Exception {

		//this.jobName = "Round4";

		super.setup(args[1], args[2]);

		
		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);


		Path path0 = new Path(args[0]);

		FileInputFormat.addInputPath(job, path0);		

		if (FileInputFormat.getInputPaths(job).length!=2){
			throw new Error("Something wrong with input paths!");
		}

		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			if(!key.toString().contains(QkCountDriver.NODE_DEGREE_SEPARATOR)) {
			
				context.write(new TextWithHashing(value.toString()), key);
			} else {
				//Input of type <u:d(u), v:d(v)>.

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
					context.write(
							new TextWithHashing(srcPair[0] + QkCountDriver.NEIGHBORLIST_SEPARATOR + dstPair[0] ),
							new Text(QkCountDriver.EDGE_EXISTS_MARKER)
							);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			CpuTimeLogger timeLog = new CpuTimeLogger("4 R", key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));

			boolean edgeFound = false;
			long triangles = 0;

			Iterator<Text> it = values.iterator();
			while (it.hasNext()){
				Text next = it.next();
				if (next.toString().equals(QkCountDriver.EDGE_EXISTS_MARKER)) edgeFound = true;
				else {
					triangles++;
				}
			}
			
			if (edgeFound && triangles>0) 
				context.write(new TextWithHashing(key.toString()),new Text(Long.toString(triangles)));

			timeLog.logCpuTime(Long.toString(triangles), edgeFound && triangles>0?"1":"0");
		}
	}
}
