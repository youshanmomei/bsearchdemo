package org.hy.hadoop.mission4.tvplay;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class TvPlayInputFormat extends FileInputFormat<Text, TvPlay>{
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, TvPlay> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new TvPlayRecordReader();
	}
	
	public class TvPlayRecordReader extends RecordReader<Text, TvPlay>{
		public LineReader in; //行读取器
		public Text lineKey; //自定义key类型
		public TvPlay lineValue; //自定义value类型
		public Text line; //每行数据类型
		
		@Override
		public void close() throws IOException {
			if(in !=null){
                in.close();
            }
		}
		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineKey;
		}
		@Override
		public TvPlay getCurrentValue() throws IOException,
				InterruptedException {
			return lineValue;
		}
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}
		@Override
		public void initialize(InputSplit input, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit)input;
			Configuration job = context.getConfiguration();
			Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            
			FSDataInputStream filein = fs.open(file);
			in = new LineReader(filein, job);
			line = new Text();
			lineKey = new Text();
			lineValue = new TvPlay();
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int linesize = in.readLine(line);// 每行数据
			if (linesize == 0)
				return false;

			String[] pieces = line.toString().split("\t");// 解析每行数据
			if (pieces.length != 7) {
				throw new IOException("Invalid record received");
			}
			
            //名字，播放频道，播放量， 收藏，评论，赞，踩，转换为 float 类型
			//继承者们	 1	4105447	202	844	48	671
            float a,b,c,d,e;
            String name = null;
            int web=-1;
            try{
            	name = pieces[0].toString().trim();
            	web = Integer.parseInt(pieces[1].trim());
            	
                a = Float.parseFloat(pieces[2].trim());
                b = Float.parseFloat(pieces[3].trim());
                c = Float.parseFloat(pieces[4].trim());
                d = Float.parseFloat(pieces[5].trim());
                e = Float.parseFloat(pieces[6].trim());
            }catch(NumberFormatException nfe){
                throw new IOException("Error parsing floating poing value in record");
            }
            
//            if(web==-1 || name==null || a==0 || b==0 || c==0 || d==0 || e==0){
//            	return false;
//            }
            
            if(web==-1 || name==null){
            	return false;
            }
            
            lineKey.set(web+"\t"+name);//完成自定义key数据
            lineValue.set(a, b, c, d, e);//封装自定义value数据
            return true;
			
		}
	}

}
