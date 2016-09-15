package org.hy.hadoop.scorecount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 学生成绩统计Hadoop类
 * 数据格式参考：19020090017 小讲 90 99 100 89 95
 * @author andy
 *
 */
public class ScoreCount extends Configured implements Tool{
	
	public static class ScoreMapper extends Mapper<Text, ScoreWritable, Text, ScoreWritable>{
		@Override
		protected void map(Text key, ScoreWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class ScoreReducer extends Reducer<Text, ScoreWritable, Text, Text>{
		private Text text = new Text();
		
		protected void reduce(Text key, Iterable<ScoreWritable> values, Context context)
			throws IOException, InterruptedException{
			float totalScore = 0.0f;
			float averageScore = 0.0f;
			
			for (ScoreWritable sw : values) {
				totalScore += sw.getChinese()+sw.getMath()+sw.getEnglish()+sw.getPhysics()+sw.getChemistry();
				
				averageScore += totalScore/5;
			}
			
			text.set(totalScore+"\t"+averageScore);
			context.write(key, text);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if (hdfs.isDirectory(myPath)) {
			hdfs.delete(myPath, true);
		}
		
		Job job = new Job(conf, "ScoreCount");
		job.setJarByClass(ScoreCount.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));//输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径
		
		job.setMapperClass(ScoreMapper.class);
		job.setReducerClass(ScoreReducer.class);
		
		job.setOutputKeyClass(Text.class); //map key输入类型
		job.setOutputValueClass(ScoreWritable.class); //map value 输出类型
		
		job.setInputFormatClass(ScoreInputFormat.class);//设置自动输入格式
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {"hdfs://hy:9000/hy/score.txt"
				, "hdfs://hy:9000/hy/score-out/"};
		int ec = ToolRunner.run(new Configuration(), new ScoreCount(), args0);
		System.exit(ec);
	}
	

}
