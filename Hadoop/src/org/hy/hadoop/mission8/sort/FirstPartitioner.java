package org.hy.hadoop.mission8.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区函数类
 * 根据first确定partitioner
 * @author andy
 *
 */
public class FirstPartitioner extends Partitioner<IntPair, IntWritable>{

	@Override
	public int getPartition(IntPair key, IntWritable value, int numPartitions) {
		return Math.abs(key.getFirst()*127)%numPartitions;//Math.abs 取绝对值
	}

}
