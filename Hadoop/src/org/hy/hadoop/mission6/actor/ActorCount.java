package org.hy.hadoop.mission6.actor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hy.hadoop.mission5.gender.Gendercount;

/**
 * 分别统计男明星和女明星的搜索热度
 * @author andy
 *
 */
public class ActorCount extends Configured implements Tool{
	public static class ActorMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			//value=name+gender+hotIndex
			String[] tokens = value.toString().split("\t");
			String gender = tokens[1].trim();
			String nameHotIndex = tokens[0] + "\t" + tokens[2];
			context.write(new Text(gender),	new Text(nameHotIndex));
		}
	}
	
	//partitioner---根据明星的性别对数据进行分区
	public static class ActorPartitioner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String sex = key.toString();
			if(numReduceTasks==0)  return 0;
			
			//sex = male -> p=0
			if(sex.equals("male")) return 0;
			
			//sex = female -> p=1
			if(sex.equals("female")) return 1%numReduceTasks;
			
			return 2%numReduceTasks;
		}
		
	}
	
	//对map端的输出结果，先进行一次合并，减少数据的网络输出
	public static class ActorCombiner extends Reducer<Text, Text, Text, Text>{
		private Text text = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int maxHotIndex = Integer.MIN_VALUE;
			int hotIndex = 0;
			String name = "";
			
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				hotIndex = Integer.parseInt(valTokens[1]);
				
				//比较得到最大的hotIndex
				if(hotIndex>maxHotIndex){
					name = valTokens[0];
					maxHotIndex = hotIndex;
				}
				
			}
			
			text.set(name+"\t"+maxHotIndex);
			context.write(key, text);
		}
		
	}
	
	
	public static class ActorReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int maxHotIndex = Integer.MIN_VALUE;
			String name = " ";
			int hotIndex = 0;
			
			for(Text val: values){
				String[] valTokens = val.toString().split("\\t");
				hotIndex = Integer.parseInt(valTokens[1]);
				
				if(hotIndex>maxHotIndex){
					name = valTokens[0];
					maxHotIndex = hotIndex;
				}
				
			}
			context.write(new Text(name), new Text(key+"\t"+maxHotIndex));
		}
	}


	@Override
	public int run(String[] args) throws Exception {
		//conf
		Configuration conf = new Configuration();
		
		//hdfs
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if (hdfs.isDirectory(myPath)) {
			hdfs.delete(myPath, true);
		}
		
		//job
		Job job = new Job(conf, "actorcount");
		job.setJarByClass(ActorCount.class);
		job.setNumReduceTasks(2);
		
		//job-conf
		job.setMapperClass(ActorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(ActorPartitioner.class);
		job.setCombinerClass(ActorCombiner.class);
		
		job.setReducerClass(ActorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//io path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//comment
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		boolean isEclipseDebug = true;
		
		String[] args0 = {"hdfs://hy:9000/mission/6-actor/actor.txt",
				"hdfs://hy:9000/mission/6-actor/out"};
		
		int ec=1;
		if(isEclipseDebug){
			ec = ToolRunner.run(new Configuration(), new ActorCount(), args0);
		}else{
			ec = ToolRunner.run(new Configuration(), new ActorCount(), args);
		}
		System.exit(ec);
	}
	

}