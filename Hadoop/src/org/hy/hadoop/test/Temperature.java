package org.hy.hadoop.test;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 统计美国每个气象站30年来平均气温
 * 1、编写map
 * 2、编写reduce
 * 3、编写run执行方法，负责mapReduce作业
 * 4、在main中运行程序
 * @author andy
 *
 */
public class Temperature extends Configured implements Tool{
	
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			
			if (temperature!=-999) {
				//得到气象站的编号
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
//				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);
				String weatherStationId = "weatherStationId";
				
				//output data
				context.write(new Text(weatherStationId), new IntWritable(temperature));
			}
			
		}
		
	}
	
	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			
			//set same temperature station per temperature
			result.set(sum/count);
			context.write(key, result);
		}
		
	}
	

	@Override
	public int run(String[] arg0) throws Exception {
		//1. read configure file
		Configuration conf = new Configuration();
		
		//2. if output path not null, delete it 
		Path myPath = new Path(arg0[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		
		//3. create job object
		Job job = new Job(conf, "temperature");
		job.setJarByClass(Temperature.class);
		
		//4. write input and output path
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		//5. write Mapper and Reducer
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		
		//6. set output style
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//7. submit job
		job.waitForCompletion(true);

		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		//1. set args0
		String[] args0 = {"hdfs://hy:9000/weather/",
				"hdfs://hy:9000/weather/out"};
		
		//2. run
		int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
		
		//3. exit by ec
		System.exit(ec);
	}
}
