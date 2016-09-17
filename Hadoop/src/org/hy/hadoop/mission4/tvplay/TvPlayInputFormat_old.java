package org.hy.hadoop.mission4.tvplay;

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
 * 根据收视率数据自定义的输入格式
 * @author andy
 *
 */
public class TvPlayInputFormat_old extends FileInputFormat<Text, TvPlay_old>{

	@Override
	public RecordReader<Text, TvPlay_old> createRecordReader(InputSplit is,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new TvPlayRecordReader();
	}
	
	public class TvPlayRecordReader extends RecordReader<Text, TvPlay_old>{
		public LineReader in; //行读取器
		public Text lineKey;
		public TvPlay_old lineValue;
		public Text line;

		@Override
		public void close() throws IOException {
			if (in!=null) {
				in.close();
			}
			
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineKey;
		}

		@Override
		public TvPlay_old getCurrentValue() throws IOException,
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
			lineValue = new TvPlay_old();
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int lineSize = in.readLine(line);//得到每行字串的长度
			if(lineSize==0)return false;
			
			//继承者们	 1	4105447	202	844	48	671
			//通过分隔符'\t'分割
			String[] pieces = line.toString().split("\t");
			
			if(pieces.length!=7){
				throw new IOException("Invalid record received");
			}
			
			String a;
			int b,c,d,e,f,g;
			try {
				a = pieces[0].toString().trim();
				b = Integer.parseInt(pieces[1].toString().trim());
				c = Integer.parseInt(pieces[2].toString().trim());
				d = Integer.parseInt(pieces[3].toString().trim());
				e = Integer.parseInt(pieces[4].toString().trim());
				f = Integer.parseInt(pieces[5].toString().trim());
				g = Integer.parseInt(pieces[6].toString().trim());
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				throw new IOException("Error parsing floating poing value in record");
			}
			
//			System.out.println("a:" +a+", b:"+b+", c:"+c+", d:"+d+", e:"+e+", f:"+f+", g:"+g);
			//自定义key和value
//			lineKey.set(TvPlay.TYPENAME[b-1]+"\t"+a);
			lineKey.set(b+"\t"+a);
			lineValue.set(b, c, d, e, f, g);
			
			return true;
		}
		
	}

}
