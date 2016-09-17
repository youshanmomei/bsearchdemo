package org.hy.hadoop.mission3.weibo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.corba.se.spi.ior.Writeable;

/**
 * 微博分析，mapReduce的主类
 * @author andy
 *
 */
public class WeiboCount extends Configured implements Tool{
	
	public static class WeiboMapper extends Mapper<Text, Weibo, Text, Text>{
		@Override
		protected void map(Text key, Weibo value, Context context) throws IOException, InterruptedException{
			context.write(new Text("follower"), new Text(key.toString()+"\t"+value.getFollowers()));
			context.write(new Text("friend"), new Text(key.toString()+"\t"+value.getFriends()));
			context.write(new Text("status"), new Text(key.toString()+"\t"+value.getStatus()));
		}
	}
	
	public static class WeiboReducer extends Reducer<Text, Text, Text, IntWritable>{
		private MultipleOutputs<Text, IntWritable> mos;
		
		protected void setup(Context context) throws IOException, InterruptedException{
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		private Text text = new Text();
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int N = context.getConfiguration().getInt("reduceHasMaxLength", Integer.MAX_VALUE);
			Map<String, Integer> m = new HashMap<String, Integer>();
			for (Text value : values) {
				//value = 名称+（粉丝数  或  关注数  或  微博数）
				String[] records = value.toString().split("\t");
				m.put(records[0], Integer.parseInt(records[1].toString()));
			}
			
			//对map内的数据进行排序
			Map.Entry<String, Integer>[] entries = getSortedHashtableByValue(m);
			
			for (int i = 0; i<N && i < entries.length; i++) {
				if (key.toString().equals("follower")) {
					mos.write("follower", entries[i].getKey(), entries[i].getValue());
				}else if(key.toString().equals("friend")){
					mos.write("friend", entries[i].getKey(), entries[i].getValue());
				}else if(key.toString().equals("status")){
					mos.write("status", entries[i].getKey(), entries[i].getValue());
				}
			}
		}
		
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
		
	}
	
	//对map内的数据进行排序
	//只适合小数据
	public static Map.Entry[] getSortedHashtableByValue(Map h){
		Set set = h.entrySet();
		Map.Entry[] entries = (Map.Entry[])set.toArray(new Map.Entry[set.size()]);
		Arrays.sort(entries, new Comparator(){
			@Override
			public int compare(Object o1, Object o2) {
				Long key1 = Long.valueOf(((Map.Entry)o1).getValue().toString());
				Long key2 = Long.valueOf(((Map.Entry)o2).getValue().toString());
				return key2.compareTo(key1);
			}});
		
		return entries;
	}

	@Override
	public int run(String[] args) throws Exception {
		//configuration
		Configuration conf = new Configuration();
		
		//file
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		//job
		Job job = new Job(conf, "weibo");
		job.setJarByClass(WeiboCount.class);
		
		//map
		job.setMapperClass(WeiboMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//reduce
		job.setReducerClass(WeiboReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//io style
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(WeiboInputFormat.class);
		
		//multiple output
		MultipleOutputs.addNamedOutput(job, "follower", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "friend", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "status", TextOutputFormat.class, Text.class, IntWritable.class);
		
		//run and end	
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {"hdfs://hy:9000/mission/3-weibo/weibo.txt",
				"hdfs://hy:9000/mission/3-weibo/weibo-out"};
		int ec = ToolRunner.run(new Configuration(), new WeiboCount(), args0);
		System.exit(ec);
	}
	

}
