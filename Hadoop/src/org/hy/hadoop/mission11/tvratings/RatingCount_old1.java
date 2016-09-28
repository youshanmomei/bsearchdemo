package org.hy.hadoop.mission11.tvratings;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 实现收视率相关指标的统计分析
 * @author andy
 *
 */
public class RatingCount_old1 {
	
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
				//将每个节目的 stbNum@name@time@分钟 -> 123456@发现之旅@2012-09-16@23:56 作为key
				//value就是这个时间内（23:56） 内的开始秒数和结束秒数 121212@121414
				for (String str : strList) {
					String[] records = str.split("@");
					if(records.length!=5) return;
					
					//stbNum + "@" + date + "@" + sn + "@" + s + "@" + e + "@" + p;
					String stbNum = records[0];//机顶盒
					String date = records[1];//日期
					String sn = records[2];//频道名
					String s = records[3];//开始时间
					String e = records[4];//结束时间
					String p = records[5];//节目名称
					
					if(s.equals("")||e.equals(""))return;
					
					int ss = TimeUtil.TimeToSecond(s);//起始时间转换为秒
					int ee = TimeUtil.TimeToSecond(e);//结束时间转换为
					if(ee<ss) ee = ee+24*3600;
					int duration = ee-ss;//每条记录的收看时长
					
					context.write(new Text(stbNum+"@"+date), new Text(sn+"@"+p+"@"+s+"@"+e+"@"+duration));
					
				}
				
			}
			
		}
	}
	
	public static class RCMapper2_MinCounter extends Mapper<Text, Text, Text, Text>{
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
		}
		
	}
	
	
	public static class RatingCountCombiner extends Reducer<Text, Text, Text, Text>{
		
		//stbNum@name@time@分钟 -> 123456@发现之旅@2012-09-16@23:56 作为key
		//values.size()	为收视率
		//结束时间-开始时间<60秒	到达人数
		//|-> value就是这个时间内（23:56） 内的收视人数和到达人数 12@12
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			int comePeople = 0;
			for (Text val : values) {
				String[] split = val.toString().split("@");
				if(split.length!=2) return;
				
				
				
				count++;
			}
		}
	}

}
