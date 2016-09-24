package org.hy.hadoop.mission10.television;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
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

public class TelevisonCombine extends Configured implements Tool{
	
	public static class TelevisionMapper extends Mapper<Object, Text, Text, MapWritable>{
		private Text text = new Text();
		
		//定义全局 Hashtable 对象
		private Hashtable<String, String> table = new Hashtable<String, String>();
		
		/**
		 * 在setup()方法中
		 * 获取分布式缓存文件channelType.csv
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String infoAddr = null;
			
			//获取分布式缓存文件路径列表
			URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
			
			//创建FileSystem
			FileSystem fs = FileSystem.get(URI.create("hdfs://hy:9000"), context.getConfiguration());
			
			//打开输入流
			FSDataInputStream in = null;
			in = fs.open(new Path(uris[0].getPath()));
			
			//创建BufferReader读取器
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
			//按行读取文件
			while(null!=(infoAddr=br.readLine())){
				String[] record = infoAddr.split("\t");
				//key 为频道名称， value为频道类别
				table.put(record[0], record[1]);
			}
			
		}
		
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//每行的数据格式 stbNum + "@" +date+ "@" +sn+ "@" +s+ "@" + e;
			String[] records = value.toString().split("@");
			String stbNum = records[0];//机顶盒
			String date = records[1];//日期
			String sn = records[2];//频道
			String s = records[3];//开始时间
			String e = records[4];//结束时间
			
			if(s.equals("")||e.equals("")){
				return;
			}
			
			String channelType = "0";//频道类别
			String ckind = table.get(sn);
			
			if(ckind!=null){
				channelType = ckind;
			}
			
			//按每条记录的 起始时间 结束时间 计算出分钟列表List
			List<String[]> list = ParseTime.getTimeSplit(s, e);//打断点
			int size = list.size();
			//循环所有分钟，拆分数据记录并输出
			for(int i=0; i<size; i++){
				String[] time = list.get(i);
				String min = time[2]; //分钟
				MapWritable avgNumMap = new MapWritable();
				avgNumMap.put(new Text(stbNum), text);
				//key=channelType + "@" + date+ "@" + min，value=avgnumMap(stbNum)
				context.write(new Text(channelType+"@"+date+"@"+min), avgNumMap);
			}
		}
		
	}
	
	public static class TelevationCombiner extends Reducer<Text, MapWritable, Text, MapWritable>{
		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable avgNumMap = new MapWritable();
			for(MapWritable val:values){
				//合并相同型号的机顶盒
				avgNumMap.putAll(val);
			}
			context.write(key, avgNumMap);
		}
	}
	
	public static class TelevisionReduce extends Reducer<Text, MapWritable, Text, Text>{
		//申明多路径输出对象
		private MultipleOutputs<Text, Text> mos;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			//输入数据为：key=channelType+date+min	value=map(stbNum)
			String[] kv = key.toString().split("@");
			String channelType = kv[0]; //频道类别
			String date = kv[1]; // 日期
			String min = kv[2]; //分钟
			MapWritable avgnumMap = new MapWritable();
			for(MapWritable m : values){
				avgnumMap.putAll(m);
			}
			
			//统计去重后的所有机顶盒数
			int avgnum = avgnumMap.size();
			//按日期将数据输出到不同文件路径下
			if(date.equals("2012-09-17")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}else if(date.equals("2012-09-18")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}else if(date.equals("2012-09-19")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}else if(date.equals("2012-09-20")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}else if(date.equals("2012-10-21")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}else if(date.equals("2012-10-22")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}else if(date.equals("2012-10-23")){
				mos.write(date.replace("-", ""), channelType, new Text(min+"\t"+avgnum));
			}
			
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//指定分布式缓存文件
		DistributedCache.addCacheFile(new URI(args[2]), conf);
		
		//namenode访问端口
		URI uri = new URI("hdfs://hy:9000");
		Path myPath = new Path(args[1]);//输出路径
		
		//创建FileSystem对象
		FileSystem hdfs = FileSystem.get(uri, conf);
		if(hdfs.isDirectory(myPath)){
			//删除输出路径
			hdfs.delete(myPath, true);
			
		}
		
		//创建一个任务
		Job job = Job.getInstance(conf, "tv");
		job.setJarByClass(TelevisonCombine.class);
		
		//输入路径
		FileInputFormat.addInputPaths(job, args[0]+"20120917,"
										+args[0]+"20120918,"
										+args[0]+"20120919,"
										+args[0]+"20120920,"
										+args[0]+"20120921,"
										+args[0]+"20120922,"
										+args[0]+"20120923");
		//输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//添加多文件夹输入
		MultipleOutputs.addNamedOutput(job, "20120917", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120918", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120919", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120920", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120921", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120922", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120923", TextOutputFormat.class, Text.class, Text.class);
		
		job.setMapperClass(TelevisionMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		
		job.setCombinerClass(TelevationCombiner.class);
		
		job.setReducerClass(TelevisionReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] arg={
				"hdfs://hy:9000/mission/10-tvJoin/tvCombined/",
				"hdfs://hy:9000/mission/10-tvJoin/ctype/",
				"hdfs://hy:9000/mission/10-tvJoin/channelType.csv"
		};
		
		int ec = ToolRunner.run(new Configuration(), new TelevisonCombine(), arg);
		System.exit(ec);
		
	}

}
