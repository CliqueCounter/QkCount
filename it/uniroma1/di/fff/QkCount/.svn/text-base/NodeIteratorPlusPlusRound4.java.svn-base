/* ============================================================================
 *  NodeIteratorPlusPlusRound4.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Triangle counter based on 
 *  						Suri, Siddharth, and Sergei Vassilvitskii. 
 *  						"Counting triangles and the curse of the last reducer." 
 *  						Proceedings of the 20th international conference on World wide web.
 *  						ACM, 2011.
 *  					
 */

package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.Checker;
import it.uniroma1.di.fff.Util.CpuTimeLogger;
import it.uniroma1.di.fff.Util.DoubleChecker;

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


public class NodeIteratorPlusPlusRound4 extends AbstractRound implements Tool {	

	boolean useLazyPairs;

	public NodeIteratorPlusPlusRound4 (boolean useLazyPairs) {
		super();
		this.useLazyPairs= useLazyPairs;
	}

	@Override
	public int run(String[] args) throws Exception {

		super.setup(args[1], args[2]);

		if(useLazyPairs) {
			this.job.setMapperClass(LazyPairMap.class);
		} else {
			this.job.setMapperClass(Map.class);
		}
		this.job.setReducerClass(Reduce.class);


		Path path0 = new Path(args[0]);

		FileInputFormat.addInputPath(job, path0);		

		if (FileInputFormat.getInputPaths(job).length!=2){
			throw new Error("Something wrong with input paths!");
		}

		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class LazyPairMap extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			String strValue =  value.toString();

			if(strValue.contains(QkCountDriver.NEIGHBORLIST_SEPARATOR)) {

				ArrayList<String> l = new ArrayList<String>();

				StringTokenizer it = new StringTokenizer(value.toString(),QkCountDriver.NEIGHBORLIST_SEPARATOR); 
				while(it.hasMoreElements()) {
					String element = it.nextToken();
					l.add(element);
				}			

				Text newValue = new Text(Checker.splitNodeAndDegree(key.toString())[0]);

				if (l.size() >=  2) {
					Collections.sort(l,new DoubleChecker());
					for (int i = 0; i < l.size(); i++) {
						for (int j= i+1; j<l.size(); j++){
							context.write(
									new Text(
											Checker.splitNodeAndDegree(l.get(i))[0]+QkCountDriver.NEIGHBORLIST_SEPARATOR+
											Checker.splitNodeAndDegree(l.get(j))[0]
											),
											newValue);
						}
					}
				}

			} else {
				//Input of type <u:d(u), v:d(v)>.

				String[] srcPair, dstPair;

				srcPair = Checker.splitNodeAndDegree(key.toString());
				dstPair = Checker.splitNodeAndDegree(strValue);

				//String src,dst;
				//int srcDegree,dstDegree;

				//src = srcPair[0];
				//dst = dstPair[0];

				//srcDegree = Integer.parseInt(srcPair[1]);

				//dstDegree = Integer.parseInt(dstPair[1]);*/

				//if(Checker.DoubleCheck(src,srcDegree,dst,dstDegree))
				context.write(
						new Text(srcPair[0] + QkCountDriver.NEIGHBORLIST_SEPARATOR + dstPair[0]),
						new Text(QkCountDriver.EDGE_EXISTS_MARKER)
						);
			}


		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		}



	}


	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {

			if(!key.toString().contains(QkCountDriver.NODE_DEGREE_SEPARATOR)) {

				context.write(value, key);
			} else {
				//Input of type <u:d(u), v:d(v)>.

				String[] srcPair, dstPair;

				srcPair = Checker.splitNodeAndDegree(key.toString());
				dstPair = Checker.splitNodeAndDegree(value.toString());

				//String src,dst;
				//int srcDegree,dstDegree;

				//src = srcPair[0];
				//dst = dstPair[0];

				//srcDegree = Integer.parseInt(srcPair[1]);

				//dstDegree = Integer.parseInt(dstPair[1]);*/

				//if(Checker.DoubleCheck(src,srcDegree,dst,dstDegree))
				context.write(
						new Text(srcPair[0] + QkCountDriver.NEIGHBORLIST_SEPARATOR + dstPair[0]),
						new Text(QkCountDriver.EDGE_EXISTS_MARKER)
						);
			}
		}
	}



	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		boolean LOG_CPUTIME;


		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			this.LOG_CPUTIME = context.getConfiguration().getBoolean(QkCountDriver.LOG_CPUTIME_CONF_KEY, false);
		}


		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			CpuTimeLogger timeLog = new CpuTimeLogger("4 R", key.toString(),
					LOG_CPUTIME);

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
				context.write(key ,new Text(Long.toString(triangles)));

			timeLog.logCpuTime(Long.toString(triangles), edgeFound && triangles>0?"1":"0");
		}
	}
}
