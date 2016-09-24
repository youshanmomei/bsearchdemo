package org.hy.hadoop.mission8.sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySort extends Configured implements Tool {
	
	
	public static class SSortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{
		private final IntPair intKey = new IntPair();
		private final IntWritable intValue = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int left=0;
			int right=0;
			
			if(tokenizer.hasMoreElements()){
				left = Integer.parseInt(tokenizer.nextToken());
				if(tokenizer.hasMoreElements()){
					right = Integer.parseInt(tokenizer.nextToken());
				}
				intKey.set(left, right);
				intValue.set(right);
				context.write(intKey, intValue);
			}
		}
		
	}
	
	public static class SSortReducer extends Reducer<IntPair, IntWritable, Text, IntWritable>{
		private final Text left = new Text();
		
		public void reduce(IntPair key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			left.set(Integer.toString(key.getFirst()));
			for (IntWritable val : values) {
				context.write(left, val);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		
		 Path myPath = new Path(args[1]);
		 FileSystem hdfs = myPath.getFileSystem(conf);
		 if(hdfs.isDirectory(myPath)){
			 hdfs.delete(myPath, true);
		 }
		 
		 Job job = new Job(conf, "secondarysort");
		 job.setJarByClass(SecondarySort.class);
		 
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapperClass(SSortMapper.class);
		 job.setReducerClass(SSortReducer.class);
		 
		 job.setPartitionerClass(FirstPartitioner.class);
		 //job.setSortComparatorClass(KeyComparator.Class);//本课程并没有自定义SortComparator，而是使用IntPair自带的排序
		 job.setGroupingComparatorClass(GroupingComparator.class);//分组函数
		 
		 job.setMapOutputKeyClass(IntPair.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		 
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		 return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		boolean isEclipseDebug = true;
		
		String[] args0 = {"hdfs://hy:9000/mission/8-sort/secondarySort.txt",
				"hdfs://hy:9000/mission/8-sort/out"};
		
		int ec=1;
		if(isEclipseDebug){
			ec = ToolRunner.run(new Configuration(), new SecondarySort(), args0);
		}else{
			ec = ToolRunner.run(new Configuration(), new SecondarySort(), args);
		}
		System.exit(ec);
	}
	

}
