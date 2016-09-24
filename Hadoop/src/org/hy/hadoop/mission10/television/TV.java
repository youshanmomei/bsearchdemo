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

/**
 * 
 * @function 使用map side join 连接频道类别 最后统计出 每天 每个类别 每分钟的收视人数 并按天分别输出不同的文件下
 * @author 小讲
 * 
 */
@SuppressWarnings("deprecation")
public class TV  extends Configured implements Tool {
	/**
	 * 
	 * @function Mapper 解析tv用户数据
	 * @input key=行偏移量  value=用户数据
	 * @output key=channelTpye+date+min  value=Map(stbNum)
	 *
	 */
	public static class TelevisionMapper extends Mapper<Object, Text, Text, MapWritable> {
		private Text text = new Text();
		//定义全局 Hashtable 对象
		private Hashtable<String, String> table = new Hashtable<String, String>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			String infoAddr = null;
			//获取分布式缓存文件列表
			URI[] uris=DistributedCache.getCacheFiles(context.getConfiguration());
			//创建FileSystem
			FileSystem fs = FileSystem.get(URI.create("hdfs://hy:9000"), context.getConfiguration());
			FSDataInputStream in = null;
			//打开输入流
			in = fs.open(new Path(uris[0].getPath()));
			//创建BufferedReader读取器
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			//按行读取文件
			while (null != (infoAddr = br.readLine())) {
				//将每行数据解析成数组 records
				String[] records = infoAddr.split("\t");
				//records[0]为频道名称，records[1]为频道类别
				table.put(records[0], records[1]);
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
		    //数据格式：stbNum + "@" + date + "@" + sn + "@" + s+ "@" + e ;
			String[] records = value.toString().split("@"); 
			String stbNum = records[0];//机顶盒
			String date = records[1];//日期
			String sn = records[2];//频道
			String s = records[3];//开始时间
			String e = records[4];//结束时间
			if(s.equals("")||e.equals("")){
				return ;
			}
			String channelType = "0";//频道类别
			String ckind = table.get(sn);
			if(ckind !=null){
				channelType = ckind;
			}
			// 按每条记录的起始时间、结束时间 计算出分钟列表List
			List<String[]> list = ParseTime.getTimeSplit(s, e);
			int size = list.size();
			//循环所有分钟，拆分数据记录并输出
			for (int i = 0; i < size; i++) {
				String[] time = list.get(i);
				String min = time[2];//分钟
				MapWritable avgnumMap = new MapWritable();
				avgnumMap.put(new Text(stbNum), text);
				context.write(new Text(channelType + "@" + date+ "@" + min), avgnumMap);
			}
		}
	}

	/**
	 * 
	 * @function 定义Combiner，合并 Mapper 输出结果 
	 *
	 */
	public static class TelevisionCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
		protected void reduce(Text key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {
			MapWritable avgnumMap = new MapWritable();
			for (MapWritable val : values) {
				//合并相同的机顶盒号
				avgnumMap.putAll(val);
			}
			context.write(key, avgnumMap);
		}
	}

	/**
	 * 
	 * @function Reducer 统计每个频道类别，每分钟的收视人数，然后按日期输出到不同文件路径下
	 * @input key=channelTpye+date+min  value=Map(stbNum)
	 * @output key=channelTpye  value=min+avgnum
	 *
	 */
	public static class TelevisionReduce extends Reducer<Text, MapWritable, Text, Text> {
		//声明多路径输出对象
		private MultipleOutputs<Text, Text> mos;

		protected void setup(Context context) throws IOException,InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		protected void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {
			//数据格式:key=channelType+date+min  value=map(stbNum)
			String[] kv = key.toString().split("@");
			String channelType = kv[0];// 频道类别
			String date = kv[1];// 日期
			String min = kv[2];// 分钟
			MapWritable avgnumMap = new MapWritable();
			for (MapWritable m : values) {
				avgnumMap.putAll(m);
			}
			//统计去重后的所有机顶盒数
			int avgnum = avgnumMap.size();
			//按日期将数据输出到不同文件路径下
			if (date.equals("2012-09-17")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			} else if (date.equals("2012-09-18")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			}else if (date.equals("2012-09-19")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			}else if (date.equals("2012-09-20")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			}else if (date.equals("2012-09-21")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			}else if (date.equals("2012-09-22")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			}else if (date.equals("2012-09-23")) {
				mos.write(date.replaceAll("-", ""), channelType, new Text(min + "\t"
						+ avgnum));
			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}
	
	/**
	 * @function 任务驱动方法
	 * @param args
	 * @return
	 * @throws Exception
	 */
	@Override
	public int run(String[] arg) throws Exception {
		// TODO Auto-generated method stub
		//读取配置文件
		Configuration conf = new Configuration();
		//指定分布式缓存文件
		DistributedCache.addCacheFile(new URI(arg[2]), conf);
        //HDFS 访问接口
		URI uri = new URI("hdfs://hy:9000");
		
		FileSystem hdfs = FileSystem.get(uri, conf);
		Path mypath = new Path(arg[1]);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = new Job(conf, "tv");
		job.setJarByClass(TV.class);//设置主类
		//输入路径
		FileInputFormat.addInputPaths(job, arg[0]+"20120917,"+arg[0]+"20120918,"+arg[0]+
				"20120919,"+arg[0]+"20120920,"+arg[0]+"20120921,"+arg[0]+"20120922,"+arg[0]+"20120923");
		//输出路径
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		// 自定义文件输出路径
		MultipleOutputs.addNamedOutput(job, "20120917", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120918", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120919", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120920", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120921", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120922", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "20120923", TextOutputFormat.class,
				Text.class, Text.class);
		
		job.setMapperClass(TelevisionMapper.class);//Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setCombinerClass(TelevisionCombiner.class);//设置Combiner

		job.setReducerClass(TelevisionReduce.class);//Reducer
		job.setOutputKeyClass(Text.class);//输出key类型
		job.setOutputValueClass(Text.class);//输出value类型
			
		job.waitForCompletion(true);//提交任务
		return 0;
	}
	
	/**
	 * @function main 方法
	 * @param args
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		String[] arg={
				"hdfs://hy:9000/mission/10-tvJoin/tvCombined/",
				"hdfs://hy:9000/mission/10-tvJoin/ctype/",
				"hdfs://hy:9000/mission/10-tvJoin/channelType.csv"
		};
		int ec = ToolRunner.run(new Configuration(), new TV(), arg);
		System.exit(ec);
	}
}
