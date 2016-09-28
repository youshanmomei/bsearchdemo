package org.hy.hadoop.mission11.tvratings;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 实现收视率相关指标的统计分析
 * @author andy
 *
 */
public class RatingCount extends Configured implements Tool{
	
	//definition a enumerate  type
	public static enum LOG_PROCESSSOR_COUNTER{
		BAD_RECORDS
	};
	
	public static class RatingCountMappper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> strList = IparseTvData.transData(value.toString());
			int length = strList.size();
			
			//invalidate record
			if(length==0){
				//dynamic definite counter
				context.getCounter("ErrorRecordCOunter", "ERROR_Record_TVData").increment(1);
				
				//enum counter
				context.getCounter(LOG_PROCESSSOR_COUNTER.BAD_RECORDS).increment(1);
			}else{
				//将每个节目的 name@time@分钟 -> 发现之旅@2012-09-16@23:56 作为key
				//value就是这个时间内（23:56） 内的开始秒数和结束秒数和机顶盒编号 121212@121414@123456
				for (String str : strList) {
					String[] records = str.split("@");
					if(records.length!=6) return;
					
					//stbNum + "@" + date + "@" + sn + "@" + s + "@" + e + "@" + p;
					String stbNum = records[0];//机顶盒
					String date = records[1];//日期
					String sn = records[2];//频道名
					String s = records[3];//开始时间
					String e = records[4];//结束时间
					String p = records[5];//节目名称
					
					if(s.equals("")||e.equals(""))return;
					
					//based on every record's start time and end time
					//get second time list
					//out: [86160, 86220, 23:56]	[初始时间对应于当天凌晨过去的秒数，当前时间对应于当天凌晨过去的秒数, 当前时间]
					List<String[]> timeList = TimeUtil.getTimeSplit(s, e);
					for (String[] t : timeList) {
						String strKey = p+"@"+date+"@"+t[2];
						String strValue = t[0]+"@"+t[1]+"@"+stbNum;
						context.write(new Text(strKey), new Text(strValue));
					}
					
				}
				
			}
			
		}
	}
	
	
	public static class RatingCountCombiner extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();
		private Set<String> set_avgnum = new HashSet<String>();
		private Set<String> set_reachNum = new HashSet<String>();
		
		//name@time@分钟 -> 发现之旅@2012-09-16@23:56 作为key
		//values.size()	为收视率
		//结束时间-开始时间<60秒	到达人数
		//|-> value就是这个时间内（23:56） 内的收视人数和到达人数 12@12
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			set_avgnum.clear();
			set_reachNum.clear();
			
			for (Text val : values) {
				String[] split = val.toString().split("@");
				if(split.length!=3) return;
				
				set_avgnum.add(split[2]);
				if(Integer.parseInt(split[1])-Integer.parseInt(split[0])>60){
					set_reachNum.add(split[2]);
				}
			}
			
			//计算出 watch num and reach num for each minutes
			result.set(set_avgnum.size()+"@"+set_reachNum.size());
			context.write(key, result);
			
		}
	}
	
	/**
	 * 统计每个节目每分钟的收视人数、到达人数、收视率、到达率、市场份额
	 * @author andy
	 *
	 */
	public static class RatingCountReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int tAvgnum = 0;
			int tReachnum = 0;
			int count = 0;
			
			Text result = new Text();
			
			for (Text value : values) {
				String[] split = value.toString().split("@");
				int avgnum = Integer.parseInt(split[0]);// 平均收视人数 || 当前在播人数
				int reachnum = Integer.parseInt(split[1]);// 平均到达人数
				
				tAvgnum += avgnum;
				tReachnum += reachnum;
				
				count++;
			}
			
			int avgWatchnum = tAvgnum/count;//收视人数
			int avgReachnum = tReachnum/count;//到达人数
			float tvrating = (float)avgWatchnum/25000*100;//收视率
			float reachrating = (float)avgReachnum/25000*100;//到达率
			float markshare = (float)avgWatchnum/25000*100;//市场份额
			
			result.set(avgWatchnum+"\t"+avgReachnum+"\t"+tvrating+"\t"+reachrating+"\t"+markshare);
			context.write(key, result);
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
//		Path myPath = new Path(args[1]);
//		FileSystem hdfs = myPath.getFileSystem(conf);
//		if(hdfs.isDirectory(myPath)){
//			hdfs.delete(myPath, true);
//		}
		//判断是否是都文件输入
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length<2){
			System.err.println("Usage: ExtractProgramNumAndTimelen [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "ratingcount");
		job.setJarByClass(RatingCount.class);
		
		job.setMapperClass(RatingCountMappper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setCombinerClass(RatingCountCombiner.class);
		
		job.setReducerClass(RatingCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置输入路径
		for(int i=0; i<otherArgs.length-1; i++){
			int d = 17;
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		
		return job.waitForCompletion(true)?0:1;
	}	 
	
	public static void main(String[] args) throws Exception {
		boolean isEclipseDebug = true;
		
//		String[] args0 = {"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-17/ars10767@20120917000000.txt",
//				"hdfs://hy:9000/mission/11-ratingcount/out"};
		String[] args0 = {
				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-17",
//				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-18",
//				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-19",
				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-20",
				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-21",
				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-22",
				"hdfs://hy:9000/mission/11-ratingcount/73/2012-09-23",
				"hdfs://hy:9000/mission/11-ratingcount/out"};
		
		int ec=1;
		if(isEclipseDebug){
			ec = ToolRunner.run(new Configuration(), new RatingCount(), args0);
		}else{
			ec = ToolRunner.run(new Configuration(), new RatingCount(), args);
		}
		System.exit(ec);
	}

}
