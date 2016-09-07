package org.hy.hadoop.api.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class JavaApiDemo {
	
	/**
	 * 获取HDFS文件系统
	 * @return
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public static FileSystem getFileSystem() throws IOException, URISyntaxException{
		//read config file
		Configuration conf = new Configuration();
		
		//返回默认文件系统
		//如果在Hadoop集群下运行，使用此种方法可以直接获取默认文件系统
		//FileSystem fs = FileSystem.get(conf);
		
		//指定的文件系统地址
		URI uri = new URI("hdfs://hy:9000");
		
		//返回指定的文件系统
		//如果在本地测试，需要使用此种方法获取文件系统
		FileSystem fs = FileSystem.get(uri, conf);
		
		return fs;
	}
	
	/**
	 * 创建文件目录
	 * @throws Exception
	 */
	public static void mkdir() throws Exception{
		//获取文件系统
		FileSystem fs = getFileSystem();
		
		//创建文件目录
		fs.mkdirs(new Path("hdfs://hy:9000/hy/weibo"));
		
		//释放资源
		fs.close();
	}
	
	/**
	 * 删除文件或者文件目录
	 * @throws Exception
	 */
	public static void rmdir() throws Exception{
		//获取文件系统
		FileSystem fs = getFileSystem();
		
		//删除文件或者文件目录
		fs.delete(new Path("hdfs://hy:9000/hy/weibo"), true);
		
		//释放资源
		fs.close();
	}
	
	
	/**
	 * 获取目录下所有文件
	 * @throws Exception
	 */
	public static void listAllFile() throws Exception{
		//获取文件系统
		FileSystem fs = getFileSystem();
		
		//列出目录内容
		FileStatus[] status = fs.listStatus(new Path("hdfs://hy:9000/hy/"));
		
		//获取目录下所有文件路径
		Path[] listedPaths = FileUtil.stat2Paths(status);
		
		//循环读取每个文件
		for (Path path : listedPaths) {
			System.out.println(path);
		}
		
		//释放资源
		fs.close();
	}
	
	/**
	 * 将文件上传至HDFS
	 * @throws Exception
	 */
	public static void copyToHDFS() throws Exception{
		//获取文件对象
		FileSystem fs = getFileSystem();
		
		//源文件路径是Linux下的路径 Path srcPath = new Path("/home/hadoop/temp.jar");
		//如果需要在windows下测试，需要改为Windows下的路径，比如 E://temp.jar
		Path srcPath = new Path("E://temp.jar");
		
		//目的路径
		Path dstPath = new Path("hdfs://hy:9000/hy/weibo");
		
		//实现文件上传
		fs.copyFromLocalFile(srcPath, dstPath);
		
		//释放资源
		fs.close();
		
	}
	
	/**
	 * 从HDFS上下载文件
	 * @throws Exception
	 */
	public static void getFile() throws Exception{
		//获得文件系统
		FileSystem fs = getFileSystem();
		
		//源文件路径
		Path srcPath = new Path("hdfs://hy:9000/hy/weibo/temp.jar");
		
		//目的路径，默认是Linux下的
		//如果在Windows下测试，需要改为Windows下的路径，如C://User/andy/Desktop/
		Path dstPath = new Path("D://");
		
		//下载HDFS上的文件
		fs.copyToLocalFile(srcPath, dstPath);
		
		//释放资源
		fs.close();
	}
	
	/**
	 * 获取HDFS集群点的信息
	 * @throws Exception
	 */
	public static void getHDFSNodes() throws Exception{
		//获取文件系统
		FileSystem fs = getFileSystem();
		
		//获取分布式文件系统
		DistributedFileSystem hdfs = (DistributedFileSystem)fs;
		
		//获取所有节点
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		
		//循环比遍历
		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println("DataNote_" + i + "_Name:" + dataNodeStats[i].getHostName());
		}
		
		//释放资源
		fs.close();
	}
	
	/**
	 * 查找某个文件在HDFS集群的位置
	 * @throws Exception
	 */
	public static void getFileLocal() throws Exception{
		//获取文件系统
		FileSystem fs = getFileSystem();
		
		//文件路径
		Path path = new Path("hdfs://hy:9000/hy/weibo/temp.jar");
		
		//获取文件目录
		FileStatus fileStatus = fs.getFileStatus(path);
		
		//获取文件块位置列表
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		
		//循环输出块信息
		for (int i = 0; i < blockLocations.length; i++) {
			String[] hosts = blockLocations[i].getHosts();
			System.out.println("block_" + i + "_location:" + hosts[0]);
		}
		
		//释放资源
		fs.close();
	}
	
	public static void main(String[] args) throws Exception {
//		FileSystem fs = getFileSystem();
//		mkdir();
//		rmdir();
//		listAllFile();
//		copyToHDFS();
//		getFile();
//		getHDFSNodes();
		getFileLocal();
		
//		System.out.println(fs);
	}
	
	

}
