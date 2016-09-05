package org.hy.hadoop.test;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 用于统计给定数据集合中的Hadoop就业薪资情况 
 * 1. mapper 
 * 2. reducer 
 * 3. realize interface run 
 * 4. run
 * 
 * @author andy
 * 
 */
public class SalaryCount extends Configured implements Tool {

	public static class SalaryMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 示例数据：美团 3-5年经验 15-30k 北京 hadoop高级工程
			String line = value.toString();
			String[] record = line.split("\\s+");// 用正则将一行数据分割（按照空格分割）

			if (record.length >= 3) {
				context.write(new Text(record[1]), new Text(record[2]));
			}

		}

	}

	public static class SalaryReducer extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int low = 0;
			int high = 0;
			int count = 1;

			for (Text value : values) {
				String[] arr = value.toString().split("-");
				int l = filterSalary(arr[0]);
				int h = filterSalary(arr[1]);

				if (count == 1 || l < low) {
					low = l;
				}

				if (count == 1 || h > high) {
					high = h;
				}

				count++;

			}
			context.write(key, new Text(low + "-" + high + "k"));

		}

		/**
		 * 过滤无效信息，提取工资值 用正则表达式提炼出数字——连续的数字
		 * 
		 * @param salary
		 * @return
		 */
		private int filterSalary(String salary) {
			String sal = Pattern.compile("[^0-9]").matcher(salary)
					.replaceAll("");
			return Integer.parseInt(sal);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		//1. read
		Configuration conf = new Configuration();
		
		//2. delete
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		//3. job
		Job job = new Job(conf, "SalaryCoyunt");
		job.setJarByClass(SalaryCount.class);
		
		//4. input and output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//5. mapper and reducer
		job.setMapperClass(SalaryMapper.class);
		job.setReducerClass(SalaryReducer.class);
		
		//6. style
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//7. submit
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
//		eclipse中的实验数据
//		String[] arg0 = {"hdfs://hy:9000/salary/salary.txt", "hdfs://hy:9000/salary/salary-out/"};
//		int ec = ToolRunner.run(new Configuration(), new SalaryCount(), arg0);
		
//		Hadoop中跑的,需要动态传入输入输出目录，最后一个参数改为args
		int ec = ToolRunner.run(new Configuration(), new SalaryCount(), args);
		System.exit(ec);
	}

}
