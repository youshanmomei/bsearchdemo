package org.hy.hadoop.mission3.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * weibo的对象类
 * @author andy
 * @version 1.0
 *
 */
public class Weibo implements WritableComparable<Object>{
	private int friends;//粉丝
	private int followers;//关注
	private int status;//微博数
	
	public int getFriends() {
		return friends;
	}
	public void setFriends(int friends) {
		this.friends = friends;
	}
	public int getFollowers() {
		return followers;
	}
	public void setFollowers(int followers) {
		this.followers = followers;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	
	public Weibo() {
		super();
	}
	
	public Weibo(int friends, int followers, int status) {
		super();
		this.friends = friends;
		this.followers = followers;
		this.status = status;
	}
	
	public void set(int friends, int followers, int status) {
		this.friends = friends;
		this.followers = followers;
		this.status = status;
	}
	
	//----------------------------------------------------------//
	//实现WritableComparable的接口
	//readFields(): 以便数据能被序列化后完成网络传输或文件输入
	//write(): 以便数据被序列化后完成网络传输或文件输出
	//----------------------------------------------------------//
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(followers);
		out.writeInt(friends);
		out.writeInt(status);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		friends = in.readInt();
		followers = in.readInt();
		status = in.readInt();
	}
	
	@Override
	public int compareTo(Object o) {
		return 0;
	}
	
	

}
