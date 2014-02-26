package it.uniroma1.di.fff.Util;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class CustomHashPartitioner<K2, V2> extends HashPartitioner<K2, V2> {

	@Override
	public int getPartition(K2 key, V2 value, int numReduceTasks) {
		// TODO Auto-generated method stub
		int res = super.getPartition(key, value, numReduceTasks);
		System.out.println("partition "+ key + " = " + res);
		return res;
	}

}
