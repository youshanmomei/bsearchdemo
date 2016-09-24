package org.hy.hadoop.mission7.tv;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hy.hadoop.mission6.actor.ActorCount;

public class TVCounter extends Configured implements Tool{
	
	//define an enumeration object
	public static enum LOG_PROCESSOR_COUNTER{
		BAD_RECORDS
	};
	
	public static class CounterAndCompressionMapper extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//parse set top box record
			List<String> list = ParseTVData.transData(value.toString());
			int length = list.size();
			
			//fail record
			if(length==0){
				//动态自定义计数器
				context.getCounter("ErrorRecordCounter", "ERROR_Record_TVData").increment(1);
				
				//枚举申明计数器
				context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS).increment(1);
			}else{
				for (String validateRecord : list) {
					//output parse user data
					context.write(new Text(validateRecord), new Text(""));
				}
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//conf
		Configuration conf = new Configuration();
		
		//path
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		//job
		Job job = new Job(conf, "CounterAndCompression");
		job.setJarByClass(TVCounter.class);
		
		
		//map
		job.setMapperClass(CounterAndCompressionMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job的输出结果进行压缩
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		//comment
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
//		boolean isEclipseDebug = true;
//		int a2 = -1;
//		try{
//			a2 = Integer.parseInt(args[2].trim());
//		}finally{
//			if(a2!=-1){
//				isEclipseDebug = false;
//			}
//		}
		
		String[] date = {"20120917", "20120918", "20120919", "20120920", "20120921", "20120922", "20120923"};
		

		
		int ec=1;
//		if(isEclipseDebug){
//			ec = ToolRunner.run(new Configuration(), new TVCounter(), args0);
//		}else{
//			ec = ToolRunner.run(new Configuration(), new TVCounter(), args);
//		}
		
		for (String dt : date) {
			String[] args0 = {"hdfs://hy:9000/mission/7-tv/" + dt + ".txt",
			"hdfs://hy:9000/mission/7-tv/out/"+dt};
			ec = ToolRunner.run(new Configuration(), new TVCounter(), args0);
		}
		System.exit(ec);
	}

}
