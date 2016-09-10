package org.hy.hadoop.api.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class UploadFile2HDFS {
	
	public static FileSystem getFileSystem() throws URISyntaxException, IOException{
		//读取配置文件
		Configuration conf = new Configuration();
		
		//指定的文件系统地址
		URI uri = new URI("hdfs://hy:9000");
		
		//返回文件系统
		return FileSystem.get(uri, conf);
		
	}
	
	public static void mkdirs(String path) throws Exception{
		FileSystem fs = getFileSystem();
		
		fs.mkdirs(new Path(path));
		
		fs.close();
	}
	
	public static void copyFromLoacal(String source, String dest) throws URISyntaxException, IOException{
		
		FileSystem fs = getFileSystem();
		
		Path srcPath = new Path(source);//源文件路径
		Path destPath = new Path(dest);//目的路径
		
		//查看目的路径是否存在
		if(!(fs.exists(destPath))){
			fs.mkdirs(destPath);
		}
		
		//得到要上传文件的文件名称
		String filename = source.substring(source.lastIndexOf("/")+1, source.length());
		
		//上传
		try {
			fs.copyFromLocalFile(srcPath, destPath);
			System.out.println("file: "+srcPath+" copied to: "+destPath);
		} catch (Exception e) {
			System.err.println("Exception caught! : " + e);
			System.exit(1);//正常退出程序
		}finally{
			fs.close();
		}
		
	}
	
	public static class RegexAcceptPathFilter implements PathFilter{
		private final String regex;
		
		public RegexAcceptPathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			// 如果要接收 regex 格式的文件，则accept()方法就return flag;
			// 如果想要过滤掉regex格式的文件，则accept()方法就return !flag;
			boolean flag = path.toString().matches(regex);
			return flag;
		}
		
	}
	

	/**
	 * 过滤文件格式，将多个文件上传至HDFS
	 * @param srcPath 本地文件输出路径
	 * @param dstPath 上传的输入路径
	 * @throws Exception
	 */
	public static void getFileList(String srcPath, String dstPath) {
		FileSystem fs = null;
		LocalFileSystem local = null;
		try {
			//得到文件系统
			Configuration conf = new Configuration();
			
			//获取Windows下的文件系统
			URI uri = new URI("hdfs://hy:9000");
			fs = FileSystem.get(uri, conf);
			
			//获取本地文件系统
			local = FileSystem.getLocal(conf);
			
			//获取文件目录
			//注意：输入路径中要有：".../data/*", 不然过滤器不起作用
			FileStatus[] loaclStatus = local.globStatus(new Path(srcPath), new RegexAcceptPathFilter("^.*txt$"));
			
			//获取所有文件路径
			Path[] listedPaths = FileUtil.stat2Paths(loaclStatus);
			
			//输出路径
			Path out = new Path(dstPath);
			
			//循环所有文件
			for (Path path : listedPaths) {
				fs.copyFromLocalFile(path, out);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception caught! : " + e);
			System.exit(1);//正常退出程序
		} finally{
			//将本地文件上传到HDFS
			try {
				if(fs!=null)fs.close();
				if(local!=null)local.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		String source = "E://项目资料/hadoop/data/205/205_data/data/*";
		String dest = "hdfs://hy:9000/hy/weibo/";
		
//		copyFromLoacal(source, dest);
		getFileList(source, dest);
	}
}
