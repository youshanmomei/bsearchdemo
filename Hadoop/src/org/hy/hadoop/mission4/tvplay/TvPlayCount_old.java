package org.hy.hadoop.mission4.tvplay;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 收视率分析的主类
 * @author andy
 * @version 1.0
 *
 */
public class TvPlayCount_old extends Configured implements Tool{
	
	public static class TvPlayMapper extends Mapper<Text, TvPlay_old, Text, TvPlay_old>{
//		@Override
//		protected void map(Text key, TvPlay value, Context context) //lineKey.set(TvPlay.typeName[b]);
//				throws IOException, InterruptedException {
////			context.write(new Text(key.toString()+"\t"+value.getName()), 
////					new Text(value.getPlaycount()+"\t"+value.getSave()+"\t"+value.getComment()+"\t"+value.getBad()+"\t"+value.getGood()));
//			context.write(new Text(key.toString()+"\t"+value.getName()), value);
//		}
		
		@Override
		protected void map(Text key, TvPlay_old value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
		
	}
	
	public static class TvPlayReducer extends Reducer<Text, TvPlay_old, Text, Text>{
		private MultipleOutputs<Text, Text> mos;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		
		private Text text = new Text();
		protected void reduce(Text key, Iterable<TvPlay_old> values, Context context) throws IOException, InterruptedException {
			
			// key = youku 继承者们
			// value = playcount save comment bad good
			int playcount=0, save=0, comment=0, bad=0, good=0;
			for (TvPlay_old value : values) {
				playcount += value.getPlaycount();
				save += value.getSave();
				comment += value.getComment();
				bad += value.getBad();
				good += value.getGood();
				
			}
			
			String[] splits = key.toString().split("\t");
//			System.out.println("splits[]: " + key);
			//这里的key是在单一文件输出的时候用来标记单一一条记录用的
			if(splits.length>1){
				mos.write(splits[0], splits[1], playcount+"\t"+save+"\t"+comment+"\t"+bad+"\t"+good);
			}
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		//conf
		Configuration conf = new Configuration();
		
		//file
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		//job
		Job job = new Job(conf, "TvPlay");
		job.setJarByClass(TvPlayCount_old.class);
		
		//map
		job.setMapperClass(TvPlayMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TvPlay_old.class);
		
		//reduce
		job.setReducerClass(TvPlayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//path and style
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(TvPlayInputFormat_old.class);
		
		//multiple output
//		MultipleOutputs.addNamedOutput(job, TvPlay.TYPENAME[0], TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, TvPlay.TYPENAME[1], TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, TvPlay.TYPENAME[2], TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, TvPlay.TYPENAME[3], TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, TvPlay.TYPENAME[4], TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "xunlei", TextOutputFormat.class, Text.class, Text.class);
		
		//run and end
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {"hdfs://hy:9000/mission/4-tvplay/tvplay.txt",
				"hdfs://hy:9000/mission/4-tvplay/tvplay-out"};
		int ec = ToolRunner.run(new Configuration(), new TvPlayCount_old(), args0);
		System.exit(ec);
	}

}
