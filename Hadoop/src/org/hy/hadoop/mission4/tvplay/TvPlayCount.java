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

public class TvPlayCount  extends Configured implements Tool{
	public final static String[] TYPENAME = { "youku", "tudou", "aiqiyi", "souhu", "xunlei" };
	
	public static class TvPlayMapper extends Mapper<Text, TvPlay, Text, TvPlay > {
        @Override
        protected void map(Text key, TvPlay value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
	
	public static class TvPlayReducer extends Reducer< Text, TvPlay, Text, Text > {
		private MultipleOutputs<Text, Text> mos;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		
        private Text text = new Text();
        protected void reduce(Text key, Iterable< TvPlay > values, Context context)
                throws IOException, InterruptedException {

            
			// key = youku 继承者们
			// value = playcount save comment bad good
			int playcount=0, save=0, comment=0, bad=0, good=0;
			for (TvPlay value : values) {
				playcount += value.getPlaycount();
				save += value.getSave();
				comment += value.getComment();
				bad += value.getBad();
				good += value.getGood();
			}
			
			String[] splits = key.toString().split("\t");
//			System.out.println("-----------> splits[]: " + key);
			//这里的key是在单一文件输出的时候用来标记单一一条记录用的
			if(splits.length>1){
				mos.write(TYPENAME[Integer.parseInt(splits[0])-1], splits[1], playcount+"\t"+save+"\t"+comment+"\t"+bad+"\t"+good);
			}
        }
        
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
    }
	
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();//读取配置文件
        
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);//创建输出路径
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        
        Job job = new Job(conf, "ScoreCount");//新建任务
        job.setJarByClass(TvPlayCount.class);//设置主类
        
        FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
        
        job.setMapperClass(TvPlayMapper.class);// Mapper
        job.setReducerClass(TvPlayReducer.class);// Reducer
        
        job.setMapOutputKeyClass(Text.class);// Mapper key输出类型
        job.setMapOutputValueClass(TvPlay.class);// Mapper value输出类型
                
        job.setInputFormatClass(TvPlayInputFormat.class);//设置自定义输入格式
        
        MultipleOutputs.addNamedOutput(job, TYPENAME[0], TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, TYPENAME[1], TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, TYPENAME[2], TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, TYPENAME[3], TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, TYPENAME[4], TextOutputFormat.class, Text.class, Text.class);
		
		//run and end
		return job.waitForCompletion(true)?0:1;
    }
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {"hdfs://hy:9000/mission/4-tvplay/tvplay.txt",
				"hdfs://hy:9000/mission/4-tvplay/tvplay-out"};
		int ec = ToolRunner.run(new Configuration(), new TvPlayCount(), args0);
		System.exit(ec);
	}

}
