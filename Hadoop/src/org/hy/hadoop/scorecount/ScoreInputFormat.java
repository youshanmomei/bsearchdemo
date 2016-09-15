package org.hy.hadoop.scorecount;

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

/**
 * 自定义学生成绩读写 InputFormat
 * 数据格式参考： 19020090017 小讲 90 99 100 89 95
 * @author andy
 *
 */
public class ScoreInputFormat extends FileInputFormat<Text, ScoreWritable>{
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, ScoreWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new ScoreRecordReader();
	}
	
	/**
	 * RecordReader 中的两个参数分别填写我们期望返回的key/value类型
	 * we expected key's style is Text, and value's style is ScoreWritable
	 * use it to package all the scores of students
	 * @author andy
	 *
	 */
	public static class ScoreRecordReader extends RecordReader<Text, ScoreWritable>{
		public LineReader in; //行读取器
		public Text linekey; //自定义key类型
		public ScoreWritable lineValue; //自定义value类型
		public Text line; //每行数据类型
		
		
		@Override
		public void close() throws IOException {
			if(in!=null){
				in.close();
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return linekey;
		}

		@Override
		public ScoreWritable getCurrentValue() throws IOException,
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
			FileSplit split = (FileSplit) input;
			Configuration job = context.getConfiguration();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			
			FSDataInputStream filein = fs.open(file);
			in = new LineReader(filein, job);
			line = new Text();
			linekey = new Text();
			lineValue = new ScoreWritable();
			
		}

		/**
		 * 此方法读取每行数据，完成自定义的key和value
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int linesize = in.readLine(line); //每行数据
			if (linesize==0) return false;
			
			String[] pieces = line.toString().split("\\s+");//解析每行数据，以空格的形式切分
			if (pieces.length!=7) {
				throw new IOException("Invalid record received");
			}
			
			//将学生每门成绩转换成float类型
			float a,b,c,d,e;
			try {
				a = Float.parseFloat(pieces[2].trim());
				b = Float.parseFloat(pieces[3].trim());
				c = Float.parseFloat(pieces[4].trim());
				d = Float.parseFloat(pieces[5].trim());
				e = Float.parseFloat(pieces[6].trim());
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				throw new IOException("Error parsing floating poing value in record");
			}
			linekey.set(pieces[0]+"\t"+pieces[1]);
			lineValue.set(a,b,c,d,e);
			
			return true;
		}
		
	}

}
