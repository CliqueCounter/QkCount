/* ============================================================================
 *  AbstractRound.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Abstract class for common configurations of mappers and reducers
*/

package it.uniroma1.di.fff.QkCount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;




public abstract class AbstractRound extends Configured implements Tool {	
	
	Job job;
	Configuration conf;
	
	@Override
	public int run(String[] args) throws Exception {
		throw new Error("Not implemented!");
	}
	
	//TO BE REMOVED!!!
	public void setup (String [] args) throws IOException {
		
		int i =args.length-2;
		this.setup(args[i],args[i+1]);

		
		Path in;
		while(i-- > 0) {
			
			in = new Path(args[i]);
			FileInputFormat.addInputPath(job,in);
		}
	}
	
	public void setup (String inputFile, String outputFile) throws IOException {

		this.conf = getConf();
		
		this.job = Job.getInstance(conf);
		
		this.job.setPartitionerClass(HashPartitioner.class);
		
		this.job.setJobName(conf.get(QkCountDriver.ROUND_JOB_NAME_CONF_KEY));
		
		job.setJarByClass(this.getClass());

		// configure output and input 
        Path in = new Path(inputFile);
        Path out = new Path(outputFile);
        FileInputFormat.addInputPath(job, in);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job,out);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		// configure output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
        
	}
}
