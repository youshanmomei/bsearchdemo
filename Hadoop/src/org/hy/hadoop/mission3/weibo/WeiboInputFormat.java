package org.hy.hadoop.mission3.weibo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * 根据微博的数据自定义的输入格式
 * @author andy
 * @version 1.0
 *
 */
public class WeiboInputFormat extends FileInputFormat<Text, Weibo>{

	@Override
	public RecordReader<Text, Weibo> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		// 这里默认是系统实现的recordReader
		// 按行读取
		// 此处用自定义的WeiboRecordReader
		return new WeiboRecordReader();
	}
	
	public class WeiboRecordReader extends RecordReader<Text, Weibo>{
		public LineReader in;
		public Text lineKey;
		public Weibo lineValue;
		public Text line;
		

		@Override
		public void close() throws IOException {
			if(in!=null){
				in.close();
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineKey;
		}

		@Override
		public Weibo getCurrentValue() throws IOException, InterruptedException {
			return lineValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit)input; //获取split
			Configuration job = context.getConfiguration();
			Path file = split.getPath();//get file path
			FileSystem fs = file.getFileSystem(job);
			
			FSDataInputStream filein = fs.open(file);//打开文件
			in = new LineReader(filein, job);
			line = new Text();
			lineKey = new Text();//新建一个Text实例作为自定义格式输入key
			lineValue = new Weibo();//新建一个Weibo实例作为自定义格式输入value
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int linesize = in.readLine(line);
			if(linesize==0)return false;
			
			//﻿俞灏明	俞灏明	10591367	206	558
			//通过分隔符'\t'
			//将每行的数据解析成pieces
			String[] pieces = line.toString().split("\t");
			if(pieces.length!=5){
				throw new IOException("Invalid record received");
			}
			
			int a,b,c;
			try {
				a = Integer.parseInt(pieces[2].trim());//粉丝
				b = Integer.parseInt(pieces[3].trim());//关注
				c = Integer.parseInt(pieces[4].trim());//微博数
			} catch (NumberFormatException nfe) {
				throw new IOException("Error parsing floating poing value in record");
			}
			
			//自定义key和value值
			lineKey.set(pieces[0]);
			lineValue.set(a, b, c);
			
			return true;
		}
		
	}
	
}
