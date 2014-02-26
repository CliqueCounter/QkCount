package it.uniroma1.di.fff.QkCount;

import it.uniroma1.di.fff.Util.TextWithHashing;

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
	//String jobName;
	
	@Override
	public int run(String[] args) throws Exception {
		throw new Error("Not implemented!");
	}
	
	public void setup (String inputFile, String outputFile) throws IOException {

		//this.conf = getConf();
		this.conf = getConf(); //new YarnConfiguration();
		
		this.job = Job.getInstance(conf);
		
		this.job.setPartitionerClass(HashPartitioner.class);
		
		//this.job.setJobName("Round" + conf.getInt(QkCountDriver.ROUND_NUMBER_CONF_KEY,  -1));
		this.job.setJobName(conf.get(QkCountDriver.ROUND_JOB_NAME_CONF_KEY));
		
		job.setJarByClass(this.getClass());
		// configure output and input source
		//String workingDir = conf.get(QkCountDriver.WORKING_DIR_CONF_KEY,"/");
        Path in = new Path(inputFile);
        Path out = new Path(outputFile);
        FileInputFormat.addInputPath(job, in);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job,out);
		job.setOutputFormatClass(TextOutputFormat.class);
		// configure mapper and reducer
		/*job.setMapperClass(StartsWithCountMapper.class);
		job.setCombinerClass(StartsWithCountReducer.class);
		job.setReducerClass(StartsWithCountReducer.class);*/
		
		
		
		// configure output
		
		
		job.setOutputKeyClass(TextWithHashing.class);
		job.setOutputValueClass(Text.class);

        //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", QkCountDriver.KEY_VALUE_PAIR_SEPARATOR);
        //conf.set("mapreduce.output.textoutputformat.separator", QkCountDriver.KEY_VALUE_PAIR_SEPARATOR);
        
	}
	
	/*@Override
	public int run(String[] args) throws Exception {
		    JobClient.runJob(job);
        return 0;
	}*/
}
