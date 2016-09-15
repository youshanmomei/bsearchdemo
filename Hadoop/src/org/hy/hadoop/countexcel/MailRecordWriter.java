package org.hy.hadoop.countexcel;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MailRecordWriter<K, V> extends RecordWriter<K, V> {
	private static final String utf8 = "UTF-8";
	private static final byte[] newline;
	
	static{
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8 + "encoding");
		}
	}
	
	protected DataOutputStream out;
	private final byte[] keyValueSeparator;
	
	public MailRecordWriter(DataOutputStream out, String keyValueSeparator) {
		this.out = out;
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes();
		} catch (Exception e) {
			throw new IllegalArgumentException();
		}
	}
	
	public MailRecordWriter(DataOutputStream out) {
		this(out, "/t");
	}
	
	private void writeObject(Object o) throws IOException{
		if (o instanceof Text) {
			Text to = (Text)o;
			out.write(to.getBytes(), 0, to.getLength());
		}else {
			out.write(o.toString().getBytes(utf8));
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		out.close();
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		boolean nullKey = key==null || key instanceof NullWritable;
		boolean nullValue = value==null || value instanceof NullWritable;
		
		if(nullKey && nullValue){
			return;
		}
		if(!nullKey){
			writeObject(key);
		}
		if (!(nullKey||nullValue)) {
			out.write(keyValueSeparator);
		}
		if(!nullValue){
			writeObject(value);
		}
		out.write(newline);
		
	}

}
