package org.hy.hadoop.mission9.join.dkej;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hy.hadoop.mission8.sort.SecondarySort;
/**
 * 两个大表
 * 通过笛卡尔积实现 reduce join
 * use station: 两个表的链接字段key都不唯一（包含一对多，多对多的关系）
 * @author andy
 *
 */
public class ReduceJoinByCartesianProduct extends Configured implements Tool{
	
	/**
	 * 为来自不同表（文件）的key/value打标签以区别不同来源的记录
	 * 然后用连接字段作为key，其余部分和新加的部分作为value，进行最后输出
	 * @author andy
	 *
	 */
	public static class ReduceJoinByCartesianProductMapper extends Mapper<Object, Text, Text, Text>{
		private Text joinKey = new Text();
		private Text combineValue = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//get path name
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			
			//if the data from records,  add a records flag
			if(pathName.endsWith("records.txt")){
				String[] valueItems = StringUtils.split(value.toString(), "\\s+");
				
				//filter fails data
				if(valueItems.length!=3){
					return;
				}
				
				joinKey.set(valueItems[0]);
				combineValue.set("records.txt"+valueItems[1]+"\t"+valueItems[2]);
			}else if(pathName.endsWith("station.txt")){
				String[] valueItems = StringUtils.split(value.toString(), "\\s+");
				if(valueItems.length!=2){
					return;
				}
				joinKey.set(valueItems[0]);
				combineValue.set("station.txt"+valueItems[1]);
			}
			
			context.write(joinKey, combineValue);
		}
	}
	
	/**
	 * reduce端做笛卡尔积
	 * @author andy
	 *
	 */
	public static class ReduceJoinByCartesianProductReduce extends Reducer<Text, Text, Text, Text>{
		private List<String> leftTable = new ArrayList<String>();
		private List<String> rightTable = new ArrayList<String>();
		private Text result = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			leftTable.clear();
			rightTable.clear();

			//same key will come into the same group
			//我们需要把相同key下来自于不同表的数据分开
			//然后做笛卡尔积
			for (Text value : values) {
				String val = value.toString();
				if(val.startsWith("station.txt")){
					leftTable.add(val.replace("station.txt", ""));
				}else if(val.startsWith("records.txt")){
					rightTable.add(val.replace("records.txt", ""));
				}
			}
			
			//笛卡尔积
			for(String leftPart: leftTable){
				for(String rightPart:rightTable){
					result.set(leftPart+"\t"+rightPart);
					context.write(key, result);
 				}
			}
		}
		
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length<2){
			System.err.println("Usage:reduceJoin<in>[<in>...] <out>");
			System.out.println(2);
		}
		
		//output path
		Path myPath = new Path(otherArgs[otherArgs.length-1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		Job job = Job.getInstance(conf, "ReduceJoinByCartesianProduct");
		job.setJarByClass(ReduceJoinByCartesianProduct.class);
		job.setMapperClass(ReduceJoinByCartesianProductMapper.class);
		job.setReducerClass(ReduceJoinByCartesianProductReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//add input path
		for(int i=0; i<otherArgs.length-1; ++i){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		
		
		//add output path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		boolean isEclipseDebug = true;
		
		String[] args0 = {"hdfs://hy:9000/mission/8-sort/secondarySort.txt",
				"hdfs://hy:9000/mission/8-sort/out"};
		
		int ec=1;
		if(isEclipseDebug){
			ec = ToolRunner.run(new Configuration(), new ReduceJoinByCartesianProduct(), args0);
		}else{
			ec = ToolRunner.run(new Configuration(), new ReduceJoinByCartesianProduct(), args);
		}
		System.exit(ec);
	}

}
