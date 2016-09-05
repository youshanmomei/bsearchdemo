package org.hy.hadoop.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * 将数据字典中相同字母组成的单词统计出来
 * @author andy
 */
public class AnagramCount extends Configured implements Tool{
	
	public static class AnagramCountMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line = value.toString();
			//process data
			String sortedWord = sortWord(line);
			//output data
			context.write(new Text(sortedWord), new Text(line));
		
		}
	}
	
	public static class AnagramCountReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			
			//此时，已经将相同sortedWord的value值封装到values的Iterable集合中了
			String result = "";
			int count = 0;
			for (Text value : values) {
				result += value + ",";
				count++;
			}
			if(count>1){//对结果集做优化，如果size=1的话，就说明不是需要的数据
				String subRes = "";
				if(result.endsWith(",")){
					subRes = result.substring(0, result.length()-1);
				}
				Text tRes = new Text(subRes); 
				
				//output data
				context.write(key, tRes);
			}
			
		}
	}

	
	/**
	 * 对单词中的字母按照字母表进行排序
	 * @param line
	 * @return
	 */
	public static String sortWord(String line) {
		//将string转换成line
		List<Character> strs = new ArrayList<Character>();
		for (int i = 0; i < line.length(); i++) {
			strs.add(line.charAt(i));
		}
		
		//用collection的sort排序，然后 输出
		Collections.sort(strs);
		String sortedWord = "";
		for (Character character : strs) {
			sortedWord += character;
		}
		
		return sortedWord;
		
	}


	@Override
	public int run(String[] arg0) throws Exception {
		//1. read
		Configuration conf = new Configuration();
		
		//2. delete
		Path myPath = new Path(arg0[1]);//看输出目录有没有文件，有的话删除
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		//3. job
		Job job = new Job(conf, "anagramCount");
		job.setJarByClass(AnagramCount.class);
		
		//4. path
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		//5. mapper and reducer
		job.setMapperClass(AnagramCountMapper.class);
		job.setReducerClass(AnagramCountReducer.class);
		
		//6. style
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//7. submit
		return job.waitForCompletion(true)?0:1;
		
	}
	
	public static void main(String[] args) throws Exception {
		String[] iargs = {"hdfs://hy:9000/anagram/665.txt", "hdfs://hy:9000/anagram/toefl-out/"};
		int ec = ToolRunner.run(new Configuration(), new AnagramCount(), iargs);
//		int ec = ToolRunner.run(new Configuration(), new AnagramCount(), args);
		System.exit(ec);
	}



}
