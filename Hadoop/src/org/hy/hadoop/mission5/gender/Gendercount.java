package org.hy.hadoop.mission5.gender;

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
import org.hy.hadoop.mission4.tvplay.TvPlayCount;

/**
 * 统计出每个阶段男女学生的最高分 需要用到自定义 Partitioner
 * 
 * @author andy
 * 
 */
public class Gendercount extends Configured implements Tool{

	public static class PCMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("<tab>");
			String gender = tokens[2].toString();
			String nameAgeScore = tokens[0] + "\t" + tokens[1] + "\t"
					+ tokens[3];
			context.write(new Text(gender), new Text(nameAgeScore));// 输出key=gender，value=name+age+score

		}

	}
	
	public static class PCPartitioner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] nameAgeScore = value.toString().split("\\t");
			
			//根据年龄进行分区
			String age = nameAgeScore[1];
			int ageInt = Integer.parseInt(age);
			if(numReduceTasks == 0){
				return 0;
			}
			
			if (ageInt<=20) {
				return 0;
			}
			
			if (ageInt>20 && ageInt<=50) {
				return 1%numReduceTasks;
			}else {
				return 2%numReduceTasks;
			}
			
		}
		
	}
	
	public static class PCReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int maxScore = Integer.MIN_VALUE;
			String name = " ";
			String age = " ";
			String gender = " ";
			int score = 0;
			
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				score = Integer.parseInt(valTokens[2]);
				if(score>maxScore){
					name = valTokens[0];
					age = valTokens[1];
					gender = key.toString();
					maxScore = score;
				}
				
			}
			
			context.write(new Text(name), new Text("age-"+age+"\tgender-"+ gender+"\tscore-"+score+maxScore));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//conf 
		Configuration conf = new Configuration();

		//hdfs
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		//job
		Job job = new Job(conf, "gender");
		job.setJarByClass(Gendercount.class);
		job.setMapperClass(PCMapper.class);
		job.setReducerClass(PCReducer.class);
		
		
		//partitioner
		job.setPartitionerClass(PCPartitioner.class);
		job.setNumReduceTasks(3);
		
		//out style
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		//commit
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {"hdfs://hy:9000/mission/5-gender/gender.txt",
				"hdfs://hy:9000/mission/5-gender/gender-out"};
		int ec = ToolRunner.run(new Configuration(), new Gendercount(), args0);
//		int ec = ToolRunner.run(new Configuration(), new Gendercount(), args);
		System.exit(ec);
	}
	

}
