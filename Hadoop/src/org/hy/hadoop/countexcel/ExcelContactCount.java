package org.hy.hadoop.countexcel;

import java.io.IOException;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统计每个月每个家庭成员与自己的通话次数
 * 并按月份输出到不同的文件
 * @author andy
 *
 */
public class ExcelContactCount extends Configured implements Tool{
	
	private static Logger logger = LoggerFactory.getLogger(ExcelContactCount.class);
	
	public static class ExcelMapper extends Mapper<LongWritable, Text, Text, Text>{
		private static Logger LOG = LoggerFactory.getLogger(ExcelMapper.class);
		private Text pkey = new Text();
		private Text pValue = new Text();
		
		/**
		 * Excel Spreadsheet is supplied  is string form to the mapper.
		 * we are simply emitting them for viewing on HDFS
		 */
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException{
			//1.0, 老爸, 13999123786, 2014-12-20
			String line = value.toString();
			String[] records = line.split("\\s+");
			String[] months = records[3].split("-");//获取月份
			pkey.set(records[1] + "\t" + months[1]);//昵称+月份
			pValue.set(records[2]);//手机号
			context.write(pkey, pValue);
			LOG.info("Map processing finished");
		}
		
	}
	
	public static class PhoneReducer extends Reducer<Text, Text, Text, Text>{
		private Text pvalue = new Text();
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			Text outkey = values.iterator().next();
			for(Text value: values){
				sum++;
			}
			pvalue.set(outkey+"\t"+sum);
			context.write(key, pvalue);
		}
		
	}
	
	//TODO...
	public static class PhoneOutputFormat extends MailMultipleOutputFormat<Text, Text>{

		@Override
		protected String generateFileNameForKeyValue(Text key, Text value,
				Configuration conf) {
			//name+month
			String[] records = key.toString().split("\t");
			return records[1]+".txt";
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		//conf
		Configuration conf = new Configuration();
		
		//path
		Path myPath = new Path(args[1]);
		FileSystem hdfs = myPath.getFileSystem(conf);//创建输出路径
		if (hdfs.isDirectory(myPath)) {
			hdfs.delete(myPath, true);
		}
		logger.info("Driver started");
		
		//job
		Job job = new Job();
		job.setJarByClass(ExcelContactCount.class);
		job.setJobName("Excel Record Reader");
		
		//mapper
		job.setMapperClass(ExcelMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(ExcelInputFormat.class);//自定义输入格式
		
		//reducer
		job.setReducerClass(PhoneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(PhoneOutputFormat.class);//自定义输出格式
		
		
		//inputpath
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
		
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {"hdfs://hy:9000/phone/phone.xls", 
				"hdfs://hy:9000/phone/phone-out"};
		int ec = ToolRunner.run(new Configuration(), new ExcelContactCount(), args0);
		System.exit(ec);
	}
	

}
