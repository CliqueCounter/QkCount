package it.uniroma1.di.fff.QkCount;


import it.uniroma1.di.fff.Util.AbstractCommand;
import it.uniroma1.di.fff.Util.ClockTimeLogger;
import it.uniroma1.di.fff.Util.CommandComputeDegrees;
import it.uniroma1.di.fff.Util.CommandConvertToAdjList;
import it.uniroma1.di.fff.Util.CommandCountCliques;
import it.uniroma1.di.fff.Util.CommandCountTriangles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;



public class QkCountDriver {

	//IO
	public static final String IO_S3_PREFIX = "s3://";
	//public static final String PERSISTENT_OUT_PREFIX = "OUT-";
	public static final String OUT1 = "out1/";
	//public static final String OUT2 = "out2";
	public static final String OUT3 = "out3/";
	public static final String OUT4 = "out4/";
	public static final String OUT5 = "out5/";
	//public static final String OUT6 = "out6";
	//FILENAMES CANNOT HAVE "_"!
	public static final String ITER_OUT_PREFIX = "ITER-";
	public static final String ITER_OUT2 = ITER_OUT_PREFIX + "out2/";
	public static final String ITER_IN = "iterIn/";
	//public static String in;

	//COMMANDS
	public static final String COMMAND_PARAM_RUN_ON_AMAZON = "-runOnAmazon";
	public static final String COMMAND_PARAM_LOG_CPUTIME = "-logCpuTime";
	public static final String COMMAND_PARAM_WORKING_DIR = "-workingDir";
	public static final String COMMAND_COMPUTE_DEGREES = "computeDegrees";
	public static final String COMMAND_PARAM_FILE_IN = "-in";
	public static final String COMMAND_PARAM_FILE_OUT = "-out";
	public static final String COMMAND_COUNT_CLIQUES = "countCliques";
	public static final String COMMAND_PARAM_CLIQUE_SIZE= "-cliqueSize";
	public static final String COMMAND_PARAM_ITERATE_ABOVE_SIZE="-iterateAbove";
	public static final String COMMAND_PARAM_EDGE_SAMPLE= "-edgeSample";
	public static final String COMMAND_PARAM_COLOR_SAMPLE= "-colorSample";
	public static final String COMMAND_COUNT_TRIANGLES= "countTriangles";
	public static final String COMMAND_TO_ADJ_LIST = "e2adj";
	public static final String COMMAND_PARAM_REDUCERS = "-reducers";
	public static final String COMMAND_PARAM_SPECULATIVE_EXEC = "-speculative";

	//SEPARATORS
	public static final String NODE_DEGREE_SEPARATOR = ":";
	public static final String NEIGHBORLIST_SEPARATOR = ",";
	public static final String KEY_VALUE_PAIR_SEPARATOR = "\t";
	public static final String NODE_HAS_SAMPLED_NEIGHBORHOOD_SEPARATOR = "#"; //MUST BE DIFFERENT FROM THE NEIGHBORLIST_SEPARATOR and NODE_DEGREE_SEPARATOR!
	public static final String GRAPH_ID_SEPARATOR = "|";
	//-----------------
	public static final String EDGE_EXISTS_MARKER = "!";

	//LOGGING
	public static final String TIME_LOG_PREAMBLE = "__TIMING_INFO__";
	public static final String TIME_LOG_SEPARATOR = KEY_VALUE_PAIR_SEPARATOR;


	//ConfKeys
	public static final String RUN_ON_AMAZON_CONF_KEY = "RUN_ON_AMAZON";
	public static final String LOG_CPUTIME_CONF_KEY = "LOG_CPU_TIME";
	public static final String WORKING_DIR_CONF_KEY = "WORKING_DIR"; 
	public static final String EDGE_SAMPLING_PROBABILITY_CONF_KEY="EDGE_SAMPLING_PROBABILITY";
	//public static final String LOG_OF_ONE_MINUS_P_CONF_KEY = "LOG_OF_ONE_MINUS_P";
	public static final String COLOR_SAMPLING_COLORS_CONF_KEY="COLOR_SAMPLING_COLORS";
	public static final String ROUND_NUMBER_CONF_KEY = "ROUND_NUMBER";
	public static final String ROUND_JOB_NAME_CONF_KEY = "ROUND_JOB_NAME";
	public static final String CLIQUE_SIZE_CONF_KEY = "CLIQUE_SIZE";
	//public static final String SAMPLING_FROM_SIZE_CONF_KEY="SAMPLING_FROM_SIZE";
	public static final String ITERATE_OVER_SIZE_CONF_KEY = "ITERATE_OVER_SIZE";
	//public static final String PARTIAL_COUNT_BEFORE_ITERATION_CONF_KEY="PARTIAL_COUNT_BEFORE_ITERATION";

	//PARAMETERS
	//public static int EDGE_SAMPLING_SEED = 1;
	public static final int MINIMUM_EXPECTED_LENGTH_FOR_SAMPLED_NEIGHBORHOODS = 10;  

	
	private static void delete(Configuration conf, FileSystem fs, String fileName, boolean recursive) throws Exception {
				
        Path toDel = new Path(buildPath(conf, fileName));
        fs.delete(toDel, recursive);
	}
	
	private static void setCommonArgs (Configuration conf, AbstractCommand c) {
		conf.setStrings(WORKING_DIR_CONF_KEY, c.getWorkingDir());
		conf.setBoolean(LOG_CPUTIME_CONF_KEY, c.getLogCpuTime());
		conf.setBoolean(RUN_ON_AMAZON_CONF_KEY, c.getRunOnAmazon());
		
		//CLUSTER SETTINGS:
        //conf.setBoolean("mapred.map.tasks.speculative.execution", c.getSpeculativeExecution());
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", c.getSpeculativeExecution());
        //conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 1.0);
        
		/*
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        conf.setInt("mapreduce.map.memory.mb", 3830);
        conf.setDouble("mapred.reduce.tasksperslot", 1.0);
        conf.setInt("mapreduce.reduce.memory.mb", 3071);
		*/
		
		conf.setInt("mapred.reduce.tasks", c.getNumReducers());
        conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 2*c.getNumReducers());
        
	}
	
	private static String buildIoPath (Configuration conf, String fileName) {
		String res = "";
		if (conf.getBoolean(QkCountDriver.RUN_ON_AMAZON_CONF_KEY, true)) {
			res = IO_S3_PREFIX + fileName;
		} else {
			res = conf.get(QkCountDriver.WORKING_DIR_CONF_KEY) + fileName;
		}
		return res;
	}
	
	public static String buildPath (Configuration conf,String fileName) {
		
		return conf.get(QkCountDriver.WORKING_DIR_CONF_KEY) + fileName;
	}
	

	public static void main(String[] args) throws Exception {

		JCommander jc = new JCommander();

		CommandComputeDegrees cComputeDegrees = new CommandComputeDegrees();
		CommandCountCliques cCountCliques = new CommandCountCliques();
		CommandCountTriangles cCountTriangles = new CommandCountTriangles();
		CommandConvertToAdjList c2Adj = new CommandConvertToAdjList();
		
		jc.addCommand(COMMAND_COMPUTE_DEGREES, cComputeDegrees);
		jc.addCommand(COMMAND_COUNT_CLIQUES, cCountCliques);
		jc.addCommand(COMMAND_COUNT_TRIANGLES, cCountTriangles); 
		jc.addCommand(COMMAND_TO_ADJ_LIST,c2Adj);
		
		jc.parse(args);

		String command = jc.getParsedCommand();

		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", QkCountDriver.KEY_VALUE_PAIR_SEPARATOR);
        conf.set("mapreduce.output.textoutputformat.separator", QkCountDriver.KEY_VALUE_PAIR_SEPARATOR);
		
        

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
				System.err.println("Counting cliques of size " + conf.getInt(CLIQUE_SIZE_CONF_KEY, -1));

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

				//ITERATION DOES NOT WORK WITH SAMPLING!
				long graphSizeForIteration = cCountCliques.getGraphSizeForIteration();
				if(graphSizeForIteration<=0) {
					if(cCountCliques.getEdgeSamplingProbability()>0) {
						System.err.println("Sampling edges with probability " + cCountCliques.getEdgeSamplingProbability() );
						conf.setDouble(EDGE_SAMPLING_PROBABILITY_CONF_KEY, cCountCliques.getEdgeSamplingProbability());
						//conf.setDouble(LOG_OF_ONE_MINUS_P_CONF_KEY, Math.log(1- cCountCliques.getEdgeSamplingProbability()));
						//conf.setInt(SAMPLING_FROM_SIZE_CONF_KEY, (int)Math.round(MINIMUM_EXPECTED_LENGTH_FOR_SAMPLED_NEIGHBORHOODS /cCountCliques.getEdgeSamplingProbability()));

					} else if(cCountCliques.getColorSampleColors() >0) {
						System.err.println("Color based edge sampling with " + cCountCliques.getColorSampleColors() + " colors");
						conf.setInt(COLOR_SAMPLING_COLORS_CONF_KEY, cCountCliques.getColorSampleColors());
						//conf.setInt(SAMPLING_FROM_SIZE_CONF_KEY, MINIMUM_EXPECTED_LENGTH_FOR_SAMPLED_NEIGHBORHOODS *cCountCliques.getColorSampleColors());

					}
				}

				round = new Round4();

				ctl = new ClockTimeLogger("4 G", "-");
				res = ToolRunner.run(conf, round, new String[]{
						QkCountDriver.buildIoPath(conf,cCountCliques.getFileIn()),
						QkCountDriver.buildPath(conf,OUT3),
						QkCountDriver.buildPath(conf,OUT4)}) + res;
				ctl.logClockTime();

				QkCountDriver.delete(conf, fs, OUT3, true);

				
				//ROUND 5 - counting!

				///long graphSizeForIteration = cCountCliques.getGraphSizeForIteration();
				if(graphSizeForIteration>0) {
					System.err.println("Iterating on graph of size " + graphSizeForIteration + " and larger");
					conf.setLong(ITERATE_OVER_SIZE_CONF_KEY, graphSizeForIteration);
				}

				conf.setInt(ROUND_NUMBER_CONF_KEY,5);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round5");

				
				if(graphSizeForIteration>0) {
					round= new Round5Iterative();
				} else {
					round= new Round5();
				}

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

				
				//Iteration!
				if(graphSizeForIteration>0) {
					//ITERROUND 1 and 2 are for computing nodes degrees
					//ITERROUND 1
					conf.setInt(ROUND_NUMBER_CONF_KEY,1);
					conf.set(ROUND_JOB_NAME_CONF_KEY, "IterRound1");

					round = new Round1And2();

					ctl = new ClockTimeLogger(outFile + " 1 IG", "-");
					res = ToolRunner.run(conf, round, new String[]{ 
							QkCountDriver.buildPath(conf,ITER_IN), 
							QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT1)});
					ctl.logClockTime();

					QkCountDriver.delete(conf, fs, ITER_IN, true);


					//ITERROUND 2
					conf.setInt(ROUND_NUMBER_CONF_KEY,2);
					conf.set(ROUND_JOB_NAME_CONF_KEY, "IterRound2");

					round = new Round1And2();

					ctl = new ClockTimeLogger(outFile + " 2 IG", "-");
					res = ToolRunner.run(conf, round, new String[]{
							QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT1),QkCountDriver.buildPath(conf, ITER_OUT2)}) + res;
					ctl.logClockTime();
					
					QkCountDriver.delete(conf, fs, ITER_OUT_PREFIX+OUT1, true);



					conf.setInt(CLIQUE_SIZE_CONF_KEY, cCountCliques.getCliqueSize()-1);
					//Reducing by 1 the size of the cliques to count because we are iterating (1 node is fixed)
					System.err.println("Iteration: counting cliques of size " + conf.getInt(CLIQUE_SIZE_CONF_KEY, -1));

					//ITERROUND 3 - creating Gamma+ neighborhoods (filtering out neighbors of small degree)
					conf.setInt(ROUND_NUMBER_CONF_KEY,3);
					conf.set(ROUND_JOB_NAME_CONF_KEY, "IterRound3");


					round = new Round3();

					ctl = new ClockTimeLogger(outFile + " 3 IG", "-");
					res = ToolRunner.run(conf, round, new String[]{QkCountDriver.buildPath(conf,ITER_OUT2),QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT3)}) + res;
					ctl.logClockTime();



					//ITERROUND 4 - small-neighborhoods intersection
					conf.setInt(ROUND_NUMBER_CONF_KEY,4);
					conf.set(ROUND_JOB_NAME_CONF_KEY, "IterRound4");

					round = new Round4();

					ctl = new ClockTimeLogger(outFile + " 4 IG", "-");
					res = ToolRunner.run(conf, round, new String[]{QkCountDriver.buildPath(conf,ITER_OUT2),QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT3),QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT4)}) + res;
					ctl.logClockTime();
					
					QkCountDriver.delete(conf, fs, ITER_OUT2, true);
					QkCountDriver.delete(conf, fs, ITER_OUT_PREFIX+OUT3, true);


					//ROUND 5 - counting!

					conf.setInt(ROUND_NUMBER_CONF_KEY,5);
					conf.set(ROUND_JOB_NAME_CONF_KEY, "IterRound5");


					round = new Round5();

					ctl = new ClockTimeLogger(outFile + " 5 IG", "-");
					res = ToolRunner.run(conf, round, new String[]{QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT4), QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT5)}) + res;
					ctl.logClockTime();
					
					QkCountDriver.delete(conf, fs, ITER_OUT_PREFIX+OUT4, true);


					//ROUND 6 - summing up!
					conf.setInt(ROUND_NUMBER_CONF_KEY,6);
					conf.set(ROUND_JOB_NAME_CONF_KEY, "IterRound6");


					round = new Round6();

					
					
					res = ToolRunner.run(conf, round, 
							new String[]{QkCountDriver.buildPath(conf,ITER_OUT_PREFIX+OUT5),
							//QkCountDriver.buildIoPath(conf,ITER_OUT_PREFIX+OUT6)})
							QkCountDriver.buildIoPath(
									conf,ITER_OUT_PREFIX+cCountCliques.getFileOut() + 
									"-" + cCountCliques.getCliqueSize())})
							+ res;
					
					QkCountDriver.delete(conf, fs, ITER_OUT_PREFIX+OUT5, true);

				}
				//endOfIteration

				ctlOverall.logClockTime();

				break;

			case COMMAND_COUNT_TRIANGLES:

				//Node Iterator ++  - ROUND 3
				conf.setInt(ROUND_NUMBER_CONF_KEY,3);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round3");

				QkCountDriver.setCommonArgs(conf, cCountTriangles);


				NodeIteratorPlusPlusRound3 nIRound3 = new NodeIteratorPlusPlusRound3();

				ctl = new ClockTimeLogger("3 G", "-");
				res = ToolRunner.run(conf, nIRound3, new String[]{QkCountDriver.buildIoPath(conf, cCountTriangles.getFileIn()),
						QkCountDriver.buildPath(conf, OUT3)}) + res;
				ctl.logClockTime();


				//Node Iterator ++  - ROUND 4
				conf.setInt(ROUND_NUMBER_CONF_KEY,4);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "Round4");


				NodeIteratorPlusPlusRound4 nIRound4 = new NodeIteratorPlusPlusRound4();

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

			case COMMAND_TO_ADJ_LIST:
				
				conf.setInt(ROUND_NUMBER_CONF_KEY,1);
				conf.set(ROUND_JOB_NAME_CONF_KEY, "e2adj");

				QkCountDriver.setCommonArgs(conf, c2Adj);
				
				conf.set("mapreduce.output.textoutputformat.separator", EdgeListToAdjLists.NODE_NEIGHBORS_SEPARATOR);

				EdgeListToAdjLists e2adj = new EdgeListToAdjLists();
				res = ToolRunner.run(conf, e2adj, new String[]{
						QkCountDriver.buildIoPath(conf, c2Adj.getFileIn()),
						QkCountDriver.buildIoPath(conf, c2Adj.getFileOut())}) + res;

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