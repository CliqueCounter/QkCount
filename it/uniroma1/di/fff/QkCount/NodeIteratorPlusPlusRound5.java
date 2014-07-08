/* ============================================================================
 *  NodeIteratorPlusPlusRound5.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Triangle counter based on 
 *  						Suri, Siddharth, and Sergei Vassilvitskii. 
 *  						"Counting triangles and the curse of the last reducer." 
 *  						Proceedings of the 20th international conference on World wide web.
 *  						ACM, 2011.
 *  					
 *  					Last round for summing up partial counts
*/


package it.uniroma1.di.fff.QkCount;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;



public class NodeIteratorPlusPlusRound5 extends AbstractRound implements Tool {	


	@Override
	public int run(String[] args) throws Exception {

		super.setup(args[0], args[1]);

		this.job.setMapperClass(Map.class);
		this.job.setReducerClass(Reduce.class);


		return this.job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key,Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text("TotCliques"), value);

		}
	}

	public static class Reduce extends Reducer <Text, Text, Text, Text> {

		@Override
		public void reduce(Text key,
				Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			long totTriangles= 0;

			Iterator<Text> it = values.iterator();
			while(it.hasNext())
				totTriangles+= Long.parseLong(it.next().toString());
			
			
			context.write(new Text("NI - 3"), new Text(Long.toString(totTriangles)));

		}
	}
}
