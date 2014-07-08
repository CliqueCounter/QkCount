/* ============================================================================
 *  QkCountDriver.java
 * ============================================================================
 * 
 *  Authors:			(c) 2014 Irene Finocchi, Marco Finocchi, Emanuele G. Fusco
 *  Description:		Main class
*/

package it.uniroma1.di.fff.QkCount;





import it.uniroma1.di.fff.Util.AbstractCommand;
import it.uniroma1.di.fff.Util.ClockTimeLogger;
import it.uniroma1.di.fff.Util.CommandComputeDegrees;
import it.uniroma1.di.fff.Util.CommandCountByJoin;
import it.uniroma1.di.fff.Util.CommandCountCliques;
import it.uniroma1.di.fff.Util.CommandCountTriangles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;



public class QkCountDriver {

	//IO
	//public static final String IO_S3_PREFIX = "s3://";
	public static final String OUT1 = "out1/";
	public static final String OUT3 = "out3/";
	public static final String OUT4 = "out4/";
	public static final String OUT5 = "out5/";
	public static final int TASK_TIMEOUT = 10800000;

	//FILENAMES CANNOT HAVE "_"!
//	public static final String ITER_OUT_PREFIX = "ITER-";
//	public static final String ITER_OUT2 = ITER_OUT_PREFIX + "out2/";
//	public static final String ITER_IN = "iterIn/";

	//COMMANDS
//	public static final String COMMAND_PARAM_RUN_ON_AMAZON = "-runOnAmazon";
	public static final String COMMAND_PARAM_LOG_CPUTIME = "-logCpuTime";
	public static final String COMMAND_PARAM_WORKING_DIR = "-workingDir";
	public static final String COMMAND_COMPUTE_DEGREES = "computeDegrees";
	public static final String COMMAND_PARAM_FILE_IN = "-in";
	public static final String COMMAND_PARAM_FILE_OUT = "-out";
	public static final String COMMAND_COUNT_CLIQUES = "countCliques";
	public static final String COMMAND_PARAM_CLIQUE_SIZE = "-cliqueSize";
//	public static final String COMMAND_PARAM_ITERATE_ABOVE_SIZE ="-iterateAbove";
	public static final String COMMAND_PARAM_EDGE_SAMPLE = "-edgeSample";
	public static final String COMMAND_PARAM_COLOR_SAMPLE = "-colorSample";
	public static final String COMMAND_COUNT_TRIANGLES = "countTriangles";
	public static final String COMMAND_COUNT_BY_JOIN = "countByJoin";
	public static final String COMMAND_PARAM_USE_L_PLUS_N = "-useLPlusN";
	public static final String COMMAND_PARAM_USE_LAZY_PAIRS = "-useLazyPairs";
	//public static final String COMMAND_PARAM_USE_TABLES = "-useTables";
	public static final String COMMAND_PARAM_BUCKETS_NUM = "-nBuckets";
	public static final String COMMAND_PARAM_REDUCERS = "-reducers";
	public static final String COMMAND_PARAM_SPECULATIVE_EXEC = "-speculative";
	public static final String COMMAND_PARAM_SPLIT_SIZE = "-splitSize";

	//SEPARATORS
	public static final String NODE_DEGREE_SEPARATOR = ":";
	public static final String NEIGHBORLIST_SEPARATOR = ",";
	public static final String PARTITIONS_SEPARATOR = ",";
	public static final String NODE_HASH_SEPARATOR = ";";
	public static final String KEY_VALUE_PAIR_SEPARATOR = "\t";
	public static final String NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR = "#";
	public static final String GRAPH_ID_SEPARATOR = "|";
	public static final String EDGE_EXISTS_MARKER = "!";

	//LOGGING
	public static final String TIME_LOG_PREAMBLE = "__TIMING_INFO__";
	public static final String TIME_LOG_SEPARATOR = KEY_VALUE_PAIR_SEPARATOR;


	//ConfKeys
//	public static final String RUN_ON_AMAZON_CONF_KEY = "RUN_ON_AMAZON";
	public static final String LOG_CPUTIME_CONF_KEY = "LOG_CPU_TIME";
	public static final String WORKING_DIR_CONF_KEY = "WORKING_DIR"; 
	public static final String EDGE_SAMPLING_PROBABILITY_CONF_KEY="EDGE_SAMPLING_PROBABILITY";
	public static final String COLOR_SAMPLING_COLORS_CONF_KEY="COLOR_SAMPLING_COLORS";
	public static final String ROUND_NUMBER_CONF_KEY = "ROUND_NUMBER";
	public static final String ROUND_JOB_NAME_CONF_KEY = "ROUND_JOB_NAME";
	public static final String CLIQUE_SIZE_CONF_KEY = "CLIQUE_SIZE";
	public static final String NUMBER_OF_BUCKETS_CONF_KEY = "NUMBER_OF_BUCKETS";
	public static final String USE_L_PLUS_N_CONF_KEY = "L_PLUS_N";
	public static final String USE_LAZY_PAIRS_CONF_KEY = "LAZY_PAIRS";

	//PARAMETERS
	public static final int MINIMUM_EXPECTED_LENGTH_FOR_SAMPLED_NEIGHBORHOODS = 10;  

	
	private static void delete(Configuration conf, FileSystem fs, String fileName, boolean recursive) throws Exception {
				
        Path toDel = new Path(buildPath(conf, fileName));
        fs.delete(toDel, recursive);
	}
	
	private static void setCommonArgs (Configuration conf, AbstractCommand c) {
		conf.setStrings(WORKING_DIR_CONF_KEY, c.getWorkingDir());
		conf.setBoolean(LOG_CPUTIME_CONF_KEY, c.getLogCpuTime());
//		conf.setBoolean(RUN_ON_AMAZON_CONF_KEY, c.getRunOnAmazon());
		
		//CLUSTER SETTINGS:
        conf.setBoolean("mapreduce.reduce.speculative", c.getSpeculativeExecution());
        conf.setBoolean("mapreduce.map.speculative", c.getSpeculativeExecution());
		conf.setInt("mapreduce.job.reduces", c.getNumReducers());
        conf.setInt("mapreduce.tasktracker.reduce.tasks.maximum", 2*c.getNumReducers());
        conf.setInt("mapreduce.input.fileinputformat.split.maxsize", c.getSplitSize());
	}
	
	private static String buildIoPath(Configuration conf, String fileName) {
		/*String res = "";
		if (conf.getBoolean(QkCountDriver.RUN_ON_AMAZON_CONF_KEY, true)) {
			res = IO_S3_PREFIX + fileName;
		} else {
			res = conf.get(QkCountDriver.WORKING_DIR_CONF_KEY) + fileName;
		}*/
		return fileName;
	}
	
	public static String buildPath (Configuration conf,String fileName) {
		
		return conf.get(QkCountDriver.WORKING_DIR_CONF_KEY) + fileName;
	}
	

	public static void main(String[] args) throws Exception {

		JCommander jc = new JCommander();

		CommandComputeDegrees cComputeDegrees = new CommandComputeDegrees();
		CommandCountCliques cCountCliques = new CommandCountCliques();
		CommandCountTriangles cCountTriangles = new CommandCountTriangles();
		CommandCountByJoin cCountByJoin = new CommandCountByJoin();
		
		jc.addCommand(COMMAND_COMPUTE_DEGREES, cComputeDegrees);
		jc.addCommand(COMMAND_COUNT_CLIQUES, cCountCliques);
		jc.addCommand(COMMAND_COUNT_TRIANGLES, cCountTriangles);
		jc.addCommand(COMMAND_COUNT_BY_JOIN, cCountByJoin);
	
		
		jc.parse(args);

		String command = jc.getParsedCommand();

		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", QkCountDriver.KEY_VALUE_PAIR_SEPARATOR);
        conf.set("mapreduce.output.textoutputformat.separator", QkCountDriver.KEY_VALUE_PAIR_SEPARATOR);
		conf.setInt("mapred.task.timeout", QkCountDriver.TASK_TIMEOUT);
        

		FileSystem fs = FileSystem.get(conf);

		if(command!=null) {

			int res = 0;
			ClockTimeLogger ctl;
			ClockTimeLogger ctlOverall = new ClockTimeLogger("Total -", "-");

			AbstractRound round;

			switch (command) {

			case COMMAND_COMPUTE_DEGREES:
				//ROUND 1 and 2 are for computing nodes degrees
				//ROUND 1
				
				//Setting the round number is MANDATORY (wrong behaviour if not set to 1!!!)
				conf.setInt(ROUND_NUMBER_CONF_KEY,1);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round1");
				QkCountDriver.setCommonArgs(conf, cComputeDegrees);
				
				
				round = new Round1And2();

				ctl = new ClockTimeLogger(cComputeDegrees.getFileOut() + " 1 G", "-");
				
				res = ToolRunner.run(conf, round, new String[]{
							QkCountDriver.buildIoPath(conf, cComputeDegrees.getFileIn()),
							QkCountDriver.buildPath(conf,OUT1)
						});
				ctl.logClockTime();


				//ROUND 2
				
				//Setting the round number is MANDATORY (wrong behaviour if not set to 2!!!)
				conf.setInt(ROUND_NUMBER_CONF_KEY,2);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round2");

				round = new Round1And2();

				ctl = new ClockTimeLogger(cComputeDegrees.getFileOut() + " 2 G", "-");
				
				res = ToolRunner.run(conf, round, new String[]{
						QkCountDriver.buildPath(conf,OUT1), 
						QkCountDriver.buildIoPath(conf, cComputeDegrees.getFileOut())
				}) + res;
				ctl.logClockTime();
				
				QkCountDriver.delete(conf, fs, OUT1, true);
				
				break;

			case COMMAND_COUNT_CLIQUES:

				conf.setInt(CLIQUE_SIZE_CONF_KEY, cCountCliques.getCliqueSize());
				conf.setBoolean(USE_L_PLUS_N_CONF_KEY, cCountCliques.getUseLPlusN());

				System.err.println("Counting cliques of size " + conf.getInt(CLIQUE_SIZE_CONF_KEY, -1));

				conf.setInt(QkCountDriver.COLOR_SAMPLING_COLORS_CONF_KEY, cCountCliques.getColorSampleColors());
				conf.setDouble(QkCountDriver.EDGE_SAMPLING_PROBABILITY_CONF_KEY, cCountCliques.getEdgeSamplingProbability());
				
				QkCountDriver.setCommonArgs(conf, cCountCliques);

				
				String outFile = QkCountDriver.buildIoPath(
						conf,cCountCliques.getFileOut() + 
						"-" + cCountCliques.getCliqueSize()
				);
				if(cCountCliques.getEdgeSamplingProbability()>0) {
					outFile = outFile + "-E"+cCountCliques.getEdgeSamplingProbability();
				}
				if(cCountCliques.getColorSampleColors()>0) {
					outFile = outFile + "-C"+cCountCliques.getColorSampleColors();
				}
				
				//ROUND 3 - creating Gamma+ neighborhoods (filtering out neighbors of small degree)
				conf.setInt(ROUND_NUMBER_CONF_KEY,3);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round3");


				round = new Round3();

				ctl = new ClockTimeLogger("3 G", "-");

				res = ToolRunner.run(conf, round, new String[]{
						QkCountDriver.buildIoPath(conf,cCountCliques.getFileIn()), 
						QkCountDriver.buildPath(conf,OUT3)}) + res;
				ctl.logClockTime();


				//ROUND 4 - small-neighborhoods intersection
				conf.setInt(ROUND_NUMBER_CONF_KEY,4);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round4");


				round = new Round4();

				ctl = new ClockTimeLogger("4 G", "-");
				//TODO: associate two distinct mappers to the distinct input sources!
				res = ToolRunner.run(conf, round, new String[]{
						QkCountDriver.buildIoPath(conf,cCountCliques.getFileIn()),
						QkCountDriver.buildPath(conf,OUT3),
						QkCountDriver.buildPath(conf,OUT4)}) + res;
				ctl.logClockTime();

				QkCountDriver.delete(conf, fs, OUT3, true);

				
				//ROUND 5 - counting!
			
				conf.setInt(ROUND_NUMBER_CONF_KEY,5);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round5");
				
				round = new Round5();
				

				ctl = new ClockTimeLogger("5 G", "-");

				res = ToolRunner.run(conf, round, new String[]{
						QkCountDriver.buildPath(conf,OUT4),
						QkCountDriver.buildPath(conf,OUT5)}) + res;
				ctl.logClockTime();
				
				QkCountDriver.delete(conf, fs, OUT4, true);


				//ROUND 6 - summing up!
				conf.setInt(ROUND_NUMBER_CONF_KEY,6);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round6");


				round = new Round6();

				res = ToolRunner.run(conf, round, new String[]{
							QkCountDriver.buildPath(conf,OUT5), 
							outFile
						}) 
						+ res;

				QkCountDriver.delete(conf, fs, OUT5, true);

				ctlOverall.logClockTime();

				break;

			case COMMAND_COUNT_TRIANGLES:

				//Node Iterator ++  - ROUND 3
				conf.setInt(ROUND_NUMBER_CONF_KEY,3);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round3");

				QkCountDriver.setCommonArgs(conf, cCountTriangles);
				
				conf.setBoolean(QkCountDriver.USE_LAZY_PAIRS_CONF_KEY, cCountTriangles.getUseLazyPairs());
				


				NodeIteratorPlusPlusRound3 nIRound3 = new NodeIteratorPlusPlusRound3(cCountTriangles.getUseLazyPairs());
				
				

				ctl = new ClockTimeLogger("3 G", "-");
				res = ToolRunner.run(conf, nIRound3, new String[]{QkCountDriver.buildIoPath(conf, cCountTriangles.getFileIn()),
						QkCountDriver.buildPath(conf, OUT3)}) + res;
				ctl.logClockTime();


				//Node Iterator ++  - ROUND 4
				conf.setInt(ROUND_NUMBER_CONF_KEY,4);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round4");


				NodeIteratorPlusPlusRound4 nIRound4 = new NodeIteratorPlusPlusRound4(cCountTriangles.getUseLazyPairs());

				ctl = new ClockTimeLogger("4 G", "-");
				res = ToolRunner.run(conf, nIRound4, new String[]{
						QkCountDriver.buildIoPath(conf, cCountTriangles.getFileIn()),
						QkCountDriver.buildPath(conf, OUT3),
						QkCountDriver.buildPath(conf, OUT4)}) + res;
				ctl.logClockTime();

				QkCountDriver.delete(conf, fs, OUT3, true);

				//Node Iterator ++  - ROUND 5
				conf.setInt(ROUND_NUMBER_CONF_KEY,5);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round5");


				NodeIteratorPlusPlusRound5 nIRound5 = new NodeIteratorPlusPlusRound5();

				ctl = new ClockTimeLogger("5 G", "-");
				res = ToolRunner.run(conf, nIRound5, new String[]{
						QkCountDriver.buildPath(conf, OUT4),
						QkCountDriver.buildIoPath(conf, cCountTriangles.getFileOut()) + "-NI"}) + res;
				ctl.logClockTime();
				
				QkCountDriver.delete(conf, fs, OUT4, true);


				ctlOverall.logClockTime();

				break;

			case COMMAND_COUNT_BY_JOIN:
					
				QkCountDriver.setCommonArgs(conf, cCountByJoin);

				
				conf.setInt(ROUND_NUMBER_CONF_KEY,1);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "URound1");
				conf.setInt(NUMBER_OF_BUCKETS_CONF_KEY , cCountByJoin.getnBuckets());
				conf.setInt(CLIQUE_SIZE_CONF_KEY, cCountByJoin.getCliqueSize());

				
				round = new URound1();
				//InputOutputPath[0] = WORKING_DIR+args[0];
				//InputOutputPath[1] = WORKING_DIR+OUT1;
				res = ToolRunner.run(conf,round,
						new String[]{
						QkCountDriver.buildIoPath(conf, cCountByJoin.getFileIn()),
						QkCountDriver.buildPath(conf,QkCountDriver.OUT1)}) +res;
				
				conf.setInt(ROUND_NUMBER_CONF_KEY,2);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "URound2");
				round = new URound2();
				//InputOutputPath[0] = WORKING_DIR+OUT1;
				//InputOutputPath[1] = WORKING_DIR+OUT2;
				res = ToolRunner.run(conf,round,
						new String[]{
						QkCountDriver.buildPath(conf,QkCountDriver.OUT1),
						QkCountDriver.buildIoPath(conf, cCountByJoin.getFileOut())}
						) + res;
				
				QkCountDriver.delete(conf, fs, QkCountDriver.OUT1, true);
				
				break;
				
			default:
				jc.usage();
				break;
			}
		} else {
			jc.usage();
			System.exit(0);
		}		

	}

}