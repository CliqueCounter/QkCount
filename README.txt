This software was compiled using version 2.2.0 of Apache™ Hadoop®!, available at

	http://hadoop.apache.org/

This software requires the jCommander library, developed by Cédric Beust and available at

	http://jcommander.org

The ideas behind this software are described in 

	Irene Finocchi, Marco Finocchi, and Emanuele Guido Fusco, 
	``Clique counting in MapReduce: theory and experiments'',
	arXiv:1403.0734 [cs.DC].
	http://arxiv.org/abs/1403.0734

Please give us credits by citing this work if you make use of our software.

HOW TO USE THIS TOOL

Usage: yarn jar <program jar file> [options] [command] [command options]

There are three main commands provided by our tool.

--------------------------------------computeDegrees--------------------------------------

Command computeDegrees computes the degrees of each node and decorates the endpoints of
each edge with their degrees.

It takes a list of edges (one edge per line, each edge as a pair of node labels 
separated by \tab). Each edge must appear only in one direction.
An example file could be:
b	a
1	5
2	5

Command computeDegrees produces in output the same list, where nodes are decorated with 
as follows:
a:1	b:1
1:1	5:2
2:1	5:2



Below is the list of options to the command computeDegrees. Required arguments are 
marked with a "*".

      Usage: computeDegrees [options]
        Options:
        * -in
             Input File
             Default: <empty string>
          -logCpuTime
             enable/disable detailed mappers and reducers cpu usage logs
             Default: false
        * -out
             Output File
             Default: <empty string>
          -reducers
             Number of reducers
             Default: 4
          -speculative
             Enabe/disable speculative execution
             Default: true
          -splitSize
             Set the split size for input files
             Default: 64
          -workingDir
             HDFS Working dir (full path)
             Default: /
             
Example: let graph.txt be the input file stored in the HDFS folder "/dataset" and let 
QkCount.jar be the jar file of the program. Consider the folling command:

	yarn jar QkCount.jar computeDegrees -in=graph.txt -out=degrees 
		-workingDir=/dataset -speculative=false -reducers=2
             
The command above would produce the HDFS folder /dataset/degrees/ containing (in two 
files, each produced by one of the two requested reducers) the list of edges decorated
with degrees. The folder "/dataset/degrees/" could then be used as input to commands 
countCliques and countTriangles, described below.

Running times, if logged, are in nanoseconds.


---------------------------------------countCliques---------------------------------------

Command countCliques computes the number of cliques of size given by parameter cliqueSize.
The cliqueSize must be at least 3. The execution becomes more computationally intensive 
as the cliqueSize becomes larger. Parameters edgeSample and colorSample allow to apply 
sampling strategies that return an approximation of the actual number of cliques. 
The input to this command should be the output folder produced by command computeDegrees.
             
      Usage: countCliques [options]
        Options:
        * -cliqueSize
             Size of the cliques to be counted
             Default: 4
          -colorSample
             Use coloring based sampling with the given number of colors
             Default: -1
          -edgeSample
             Use edge sampling with the given probability
             Default: -1.0
        * -in
             Input File
             Default: <empty string>
          -logCpuTime
             enable/disable detailed mappers and reducers cpu usage logs
             Default: false
        * -out
             Output File
             Default: <empty string>
          -reducers
             Number of reducers
             Default: 4
          -speculative
             Enabe/disable speculative execution
             Default: true
          -splitSize
             Set the split size for input files
             Default: 64
          -useLPlusN
             Use the LPlusN clique count algorithm if true, NodeIterator++ if
             false
             Default: true
          -workingDir
             HDFS Working dir (full path)
             Default: /

Example:
	yarn jar QkCount.jar countCliques -in=degrees -out=cliques -cliqueSize=4 
			-workingDir=/dataset

The command above would produce the HDFS folder /dataset/cliques-4/ containing a non-empty
file with the algorithms acronym, the sampling probability or the number of colors for the 
approximate algorithm, the cliqueSize given in input, and the computed number of cliques.

Algorithm acromyms are:
X = exact
C = approximation algorithm using color-based sampling
E = approximation algorithm using simple edge sampling
We refer to our paper for details on these algorithms.


--------------------------------------countTriangles--------------------------------------

Command countTriangles applies the algorithm NodeIterator++ described in 

	Siddharth Suri and Sergei Vassilvitskii, 
	"Counting triangles and the curse of the last reducer", 
	Proceedings of the 20th international conference on World wide web, ACM, 2011

and returns the number of triangles in the input graph. The input to this command should 
be the output folder produced by command computeDegrees.

      Usage: countTriangles [options]
        Options:
        * -in
             Input File
             Default: <empty string>
          -logCpuTime
             enable/disable detailed mappers and reducers cpu usage logs
             Default: false
        * -out
             Output File
             Default: <empty string>
          -reducers
             Number of reducers
             Default: 4
          -runOnAmazon
             Execution on Amazon EMR (sets IO on s3 filesystem)
             Default: true
          -speculative
             Enabe/disable speculative execution
             Default: true
          -splitSize
             Set the split size for input files
             Default: 64
          -useLazyPairs
             Use the lazy pair explosion
             Default: false
          -workingDir
             HDFS Working dir (full path)
             Default: /
             
Example:
	yarn jar QkCount.jar countTriangles -in=degrees -out=triangles 
			-workingDir=/dataset

----------------------------------------countByJoin---------------------------------------
Command countByJoin applies the multiway join algorithm described in


	Foto N. Afrati, Dimitris Fotakis, and Jeffrey D. Ullman
	Enumerating subgraph instances using map-reduce
	Proceedings of the 29th IEEE International Conference on Data Engineering ICDE 2013

      Usage: countByJoin [options]
        Options:
        * -cliqueSize
             Size of the cliques to be counted
             Default: 4
        * -in
             Input File
             Default: <empty string>
          -logCpuTime
             enable/disable detailed mappers and reducers cpu usage logs
             Default: false
        * -nBuckets
             Number of buckets to use for the multiway join
             Default: 4
        * -out
             Output File
             Default: <empty string>
          -reducers
             Number of reducers
             Default: 4
          -speculative
             Enabe/disable speculative execution
             Default: false
          -splitSize
             Set the split size for input files
             Default: 64
          -workingDir
             HFS Working dir (full path)
             Default: /

Example:
	yarn jar QkCount.jar countByJoin -in=degrees -out=triangles -cliqueSize=4 
			-workingDir=/dataset -nBuckets=8
