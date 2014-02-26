package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.DoubleChecker;
import it.uniroma1.di.fff.Util.TextWithHashing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;


public class Round4 extends AbstractRound implements Tool {	

	@Override
	public int run(String[] args) throws Exception {

		//this.jobName = "Round4";

		super.setup(args[1], args[2]);

		if(conf.getDouble(QkCountDriver.EDGE_SAMPLING_PROBABILITY_CONF_KEY, -1) > 0) {
			this.job.setMapperClass(MapEdgeSampler.class);
		} else {
			if(conf.getInt(QkCountDriver.COLOR_SAMPLING_COLORS_CONF_KEY,-1)>0) {
				this.job.setMapperClass(MapColorSampler.class);
			} else {
				this.job.setMapperClass(Map.class);
			}
		}
		this.job.setReducerClass(Reduce.class);

		//String workingDir = conf.get(QkCountDriver.WORKING_DIR_CONF_KEY,"/");
		Path path0 = new Path(args[0]);

		FileInputFormat.addInputPath(job, path0);		

		if (FileInputFormat.getInputPaths(job).length!=2){
			throw new Error("Something wrong with input paths!");
		}

		return this.job.waitForCompletion(true) ? 0 : 1;
	}


	public static void commonMapFragment (Text key,Text value, org.apache.hadoop.mapreduce.Mapper<Text, Text, TextWithHashing, Text>.Context context) throws IOException, InterruptedException  {
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
					new TextWithHashing(srcPair[0] /*+ QkCountDriver.NODE_DEGREE_SEPARATOR + srcPair[1]*/ + QkCountDriver.NEIGHBORLIST_SEPARATOR + dstPair[0] /*+ QkCountDriver.NODE_DEGREE_SEPARATOR + dstPair[1]*/),
					new Text(QkCountDriver.EDGE_EXISTS_MARKER)
					);
	}
	public static class MapEdgeSampler extends Mapper <Text, Text, TextWithHashing, Text> {
		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			String sValue = value.toString();

			if(sValue.contains(QkCountDriver.NEIGHBORLIST_SEPARATOR)) {
				//Input of type <u:d(u), Gamma+(u)>

				//We log only mapper handling neighborlists, not those handling single edges
				//NOTICE THAT the  time used for testing the input time is not logged
				CpuTimeLogger timeLog = new CpuTimeLogger("4 M", key.toString(),
						context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));


				ArrayList<String> neighbors = new ArrayList<String>();



				//Input of type <u:d(u), Gamma+(u)>
				StringTokenizer it = new StringTokenizer(value.toString(),QkCountDriver.NEIGHBORLIST_SEPARATOR); 
				while(it.hasMoreElements()) {
					String element = it.nextToken();
					neighbors.add(element);
				}


				Collections.sort(neighbors, new DoubleChecker());
				long prunedSize = 0;

				Configuration conf = context.getConfiguration();
				//
				/*if(Double.isNaN(logOfOneMinusP)) {
					throw new Error("logOfOneMinusP is NaN!");
				}*/

				Text newValue;

				//int samplingFromSize=conf.getInt(QkCountDriver.SAMPLING_FROM_SIZE_CONF_KEY,-1);
				int samplingFromSize=QkCountDriver.MINIMUM_EXPECTED_LENGTH_FOR_SAMPLED_NEIGHBORHOODS;
				
				long max = ((neighbors.size()-1)*neighbors.size())/2;

				if(max<=samplingFromSize) {
					newValue = new Text(Checker.splitNodeAndDegree(key.toString())[0]+QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR+1.0);

					prunedSize=max;
					for(int i= 0; i< neighbors.size(); i++){
						for (int j = i+1 ; j<neighbors.size(); j++){
							context.write(new TextWithHashing(Checker.splitNodeAndDegree(neighbors.get(i))[0] + 
									QkCountDriver.NEIGHBORLIST_SEPARATOR + 
									Checker.splitNodeAndDegree(neighbors.get(j))[0]), 
									newValue);
						}
					}
				} else {
					int i=0;
					int j=0;

					double esp = Math.max(conf.getDouble(QkCountDriver.EDGE_SAMPLING_PROBABILITY_CONF_KEY, -1), samplingFromSize/max);
					double logOfOneMinusP = Math.log(1- esp); //conf.getDouble(QkCountDriver.LOG_OF_ONE_MINUS_P_CONF_KEY, Double.NaN);

					newValue = new Text(Checker.splitNodeAndDegree(key.toString())[0]+QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR+esp);

					int k = 1 + (int)Math.floor(Math.log(1-Math.random())/logOfOneMinusP);

					while(k <= max) {


						i=(int)Math.ceil((Math.sqrt(1+8*k)-1)/2.0);

						j= k-(((i-1)*i)/2);

						int indexI = neighbors.size()-i-1;
						int indexJ = neighbors.size()-j;

						/*debug
					if(indexI>=indexJ){
						throw new Error("indexI>=indexJ - " + indexI +" - " + indexJ);
					}
					if(i<0 ||j<=0 || indexI >= neighbors.size() || indexJ>=neighbors.size()) {
						throw new Error("LessThan0! indexI=" +indexI + 
								" neighbors.size()=" + neighbors.size() +
								" i=" + i + " indexJ=" + indexJ + " j=" +j +
								" k=" + k + " max=" + max);
					}*/

						prunedSize ++;
						context.write(new TextWithHashing(Checker.splitNodeAndDegree(neighbors.get(indexI))[0] + 
								QkCountDriver.NEIGHBORLIST_SEPARATOR + 
								Checker.splitNodeAndDegree(neighbors.get(indexJ))[0]), 
								newValue);

						k += 1+(int)Math.floor(Math.log(1-Math.random())/logOfOneMinusP);
					}
					timeLog.logCpuTime(Integer.toString(neighbors.size()),Long.toString(prunedSize));
				}

			} else {
				//Input of type <u:d(u), v:d(v)>.

				Round4.commonMapFragment(key, value, context);

			}

		}
	}


	public static class MapColorSampler extends Mapper <Text, Text, TextWithHashing, Text> {
		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			String sValue = value.toString();

			if(sValue.contains(QkCountDriver.NEIGHBORLIST_SEPARATOR)) {
				//Input of type <u:d(u), Gamma+(u)>

				//We log only mapper handling neighborlists, not those handling single edges
				//NOTICE THAT the  time used for testing the input time is not logged
				CpuTimeLogger timeLog = new CpuTimeLogger("4 M", key.toString(),
						context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));


				Configuration conf= context.getConfiguration();
				


				int prunedSize;

				StringTokenizer it = new StringTokenizer(value.toString(),QkCountDriver.NEIGHBORLIST_SEPARATOR); 
				ArrayList<String>allNeighbors = new ArrayList<String>();
				while(it.hasMoreElements()) {
					allNeighbors.add(it.nextToken());
				}
				int overallSize = allNeighbors.size();

				Text newValue;

				//int samplingFromSize=conf.getInt(QkCountDriver.SAMPLING_FROM_SIZE_CONF_KEY,-1);
				int samplingFromSize=QkCountDriver.MINIMUM_EXPECTED_LENGTH_FOR_SAMPLED_NEIGHBORHOODS;
				int max = overallSize*(overallSize-1)/2;

				if(max<=samplingFromSize) {
				//if(overallSize*(overallSize-1)/2<=samplingFromSize) {
					prunedSize= max;
					newValue = new Text(Checker.splitNodeAndDegree(key.toString())[0]+QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR+1);
					
					//TreeSet<String> allNeighbors = new TreeSet<String>(new DoubleChecker());  
					Collections.sort(allNeighbors, new DoubleChecker());
					for(int i= 0; i< allNeighbors.size(); i++){
						for (int j = i+1 ; j<allNeighbors.size(); j++){
							context.write(new TextWithHashing(Checker.splitNodeAndDegree(allNeighbors.get(i))[0] + 
									QkCountDriver.NEIGHBORLIST_SEPARATOR + 
									Checker.splitNodeAndDegree(allNeighbors.get(j))[0]), 
									newValue);
						}
					}

				} else {
					int nColors = Math.min(conf.getInt(QkCountDriver.COLOR_SAMPLING_COLORS_CONF_KEY,1), (int)Math.round((double)max/(double)samplingFromSize));


					@SuppressWarnings("unchecked")
					ArrayList<String> [] neighbors = new ArrayList[nColors];

					for (int i = 0; i < nColors; i++){
						neighbors[i] = new ArrayList<String>();
					}
					
					for (int i = 0; i<allNeighbors.size();i++){
						neighbors[(int)Math.floor(Math.random()*nColors)].add(allNeighbors.get(i));
					}

					newValue = new Text(Checker.splitNodeAndDegree(key.toString())[0]+QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR+nColors);
					prunedSize = 0;
					for(int h = 0; h<nColors; h++){
						Collections.sort(neighbors[h], new DoubleChecker());
						for(int i= 0; i< neighbors[h].size(); i++){
							for (int j = i+1 ; j<neighbors[h].size(); j++){
								prunedSize ++;
								context.write(new TextWithHashing(Checker.splitNodeAndDegree(neighbors[h].get(i))[0] + 
										QkCountDriver.NEIGHBORLIST_SEPARATOR + 
										Checker.splitNodeAndDegree(neighbors[h].get(j))[0]), 
										newValue);
							}
						}
					}
				}
				timeLog.logCpuTime(Long.toString(overallSize), Long.toString(prunedSize));

			} else {
				//Input of type <u:d(u), v:d(v)>.

				Round4.commonMapFragment(key, value, context);

			}

		}
	}

	public static class Map extends Mapper<Text, Text, TextWithHashing, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			String sValue = value.toString();

			if(sValue.contains(QkCountDriver.NEIGHBORLIST_SEPARATOR)) {
				//Input of type <u:d(u), Gamma+(u)>

				//We log only mapper handling neighborlists, not those handling single edges
				//NOTICE THAT the  time used for testing the input time is not logged
				CpuTimeLogger timeLog = new CpuTimeLogger("4 M", key.toString(),
						context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));

				ArrayList<String> neighbors = new ArrayList<String>();

				StringTokenizer it = new StringTokenizer(value.toString(),QkCountDriver.NEIGHBORLIST_SEPARATOR); 
				while(it.hasMoreElements()) {
					String element = it.nextToken();
					neighbors.add(element);
				}

				Collections.sort(neighbors, new DoubleChecker());
				
				Text newValue = new Text(Checker.splitNodeAndDegree(key.toString())[0]/*+QkCountDriver.NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR+1.0*/);

				for(int i= 0; i< neighbors.size(); i++){
					for (int j = i+1 ; j<neighbors.size(); j++){
						//context.write(new Text(neighbors.get(i) + QkCountDriver.NEIGHBORLIST_SEPARATOR + neighbors.get(j)), key);
						context.write(new TextWithHashing(Checker.splitNodeAndDegree(neighbors.get(i))[0] + 
								QkCountDriver.NEIGHBORLIST_SEPARATOR + 
								Checker.splitNodeAndDegree(neighbors.get(j))[0]), 
								newValue);
					}

				}
				timeLog.logCpuTime(Integer.toString(neighbors.size()), Long.toString(neighbors.size()*(neighbors.size()-1)/2));

			} else {
				//Input of type <u:d(u), v:d(v)>.

				Round4.commonMapFragment(key, value, context);

			}
		}
	}

	public static class Reduce extends Reducer<TextWithHashing, Text, TextWithHashing, Text> {

		@Override
		public void reduce(TextWithHashing key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			CpuTimeLogger timeLog = new CpuTimeLogger("4 R", key.toString(),
					context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, true));

			StringBuffer sb = new StringBuffer();

			boolean edgeFound = false;
			int neighListSize = 0;

			Iterator<Text> it = values.iterator();
			while (it.hasNext()){
				Text next = it.next();
				if (next.toString().equals(QkCountDriver.EDGE_EXISTS_MARKER)) edgeFound = true;
				else {
					neighListSize++;
					sb.append(next);
					sb.append(QkCountDriver.NEIGHBORLIST_SEPARATOR);
				}
			}
			if (sb.length()>0) {
				sb.setLength(sb.length() - QkCountDriver.NEIGHBORLIST_SEPARATOR.length());
			}

			if (edgeFound && sb.length()>0) 
				context.write(new TextWithHashing(key.toString()),new Text(sb.toString()));

			timeLog.logCpuTime(Integer.toString(neighListSize), edgeFound && sb.length()>0?"1":"0");
		}
	}
}
