package org.hy.hadoop.api.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

/**
 * 小文件合并
 * 1.过滤掉.svn格式的文件
 * 2.循环所有文件, 通过globalStatus获取所有.txt格式路径
 * 3.通过IOUtils.copyByBytes()将数据集合并为7个文件
 * 4.上传至HDFS
 * @author andy
 */
public class CombineSmallFile {
	
	private static Configuration conf;

	/**
	 * 封装的过滤的类
	 * @author andy
	 *
	 */
	public static class RegexPathFilter implements PathFilter{
		public static final int MOOD_FILTER = 0;//将匹配的文件删除
		public static final int MOOD_SELECT = 1;//将匹配的文件保留
		
		private final String regex;
		private final int mood;
		
		public RegexPathFilter(String regex, int mood) {
			this.regex = regex;
			this.mood = mood;
		}

		@Override
		public boolean accept(Path path) {
			boolean flag = false;
			if(mood==0){
				flag = !path.toString().matches(regex);
			}else{
				flag = path.toString().matches(regex);
			}
			return flag;
		}
		
	}
	
	/**
	 * 合并上传小文件
	 * 
	 * @param srcPath 要输入的文件的目录路径
	 * @param dstPath 要输出的文件的目录
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public static void combineFileUploadList(String srcPath, String dstPath) throws IOException, URISyntaxException{
		//获取HDFS的文件系统
//		URI uri = new URI("hdfs://hy:9000");
//		FileSystem fs = FileSystem.get(uri, conf);
		//Hadoop集群下
		FileSystem fs = FileSystem.get(conf);
		
		//获取本地的文件系统
		LocalFileSystem local = FileSystem.getLocal(conf);
		
		FileStatus[] dirStatus = local.globStatus(new Path(srcPath), new RegexPathFilter("^.*svn$", RegexPathFilter.MOOD_FILTER));
		Path[] paths = FileUtil.stat2Paths(dirStatus);//获得所有文件路径
		FSDataInputStream fsdis = null;
		FSDataOutputStream fsdos = null;
		
		//检查输出目录
		if(!(fs.exists(new Path(dstPath)))){
			fs.mkdirs(new Path(dstPath));
		}
		
		for (Path dir : paths) {
			String fileName = dir.getName();
			//只接受日期目录下的.txt文件
			FileStatus[] localStatus = local.globStatus(new Path(srcPath+fileName+"/*"), new RegexPathFilter("^.*txt$", RegexPathFilter.MOOD_SELECT));
			//获得日期目录下的所有文件
			Path[] listedPaths = FileUtil.stat2Paths(localStatus);
			//输出路径
			Path block = new Path(dstPath+getFileName(fileName));
			//打开输出流
			if(!(fs.exists(block))){
				fsdos = fs.create(block);
			}else{
				//打开输出流
				fsdos = fs.append(block); 
			}
			
			
			for (Path path : listedPaths) {
				fsdis = local.open(path);//此处要用loacl的文件目录
				IOUtils.copyBytes(fsdis, fsdos, 4096, false);
				fsdis.close();
			}
			fsdos.close();
			
		}
		
		fs.close();
		
	}
	
	/**
	 * 根据文件夹的名字得到合并后的文件名
	 * 2012-09-17 -> 20130917.txt
	 * @param dirName
	 * @return
	 */
	private static String getFileName(String dirName) {
		String repStr = dirName.replace("-", "");
		return repStr+".txt";
	}

	/**
	 * 做一些初始化的操作
	 */
	public static void init(){
		if(conf==null)conf = new Configuration();	
	}
	
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		String source = "E://项目资料/hadoop/data/73/*";
		String dest = "hdfs://hy:9000/hy/tv/";
		
		init();
		combineFileUploadList(source, dest);
	}


	
	

}
