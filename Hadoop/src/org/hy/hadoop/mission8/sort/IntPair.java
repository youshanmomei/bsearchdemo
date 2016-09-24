package org.hy.hadoop.mission8.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义的key类，应该实现WritableComparable
 * @author andy
 *
 */
public class IntPair implements WritableComparable<IntPair>{
	int first;
	int second;
	
	public void set(int left, int right){
		this.first = left;
		this.second = right;
	}
	
	public int getFirst() {
		return first;
	}

	
	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}


	/**
	 * 序列化时
	 * 将IntPair转换成二进制时调用
	 * 该二进制用于流传送
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}


	/**
	 * 反序列化时
	 * 从流中的二进制转换成IntPair时调用
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	/**
	 * key的比较时调用
	 * 相等 		0
	 * 本地的小	-1
	 * 本地的大	1
	 */
	@Override
	public int compareTo(IntPair o) {
		if(first!=o.first){
			return first<o.first?-1:1;
		}else if (second !=o.second) {
			return second<o.second?-1:1;
		}else{
			return 0;
		}
	}
	
	@Override
	public int hashCode() {
		return first*157+second;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj==null) return false;
		if(this==obj) return true;
		
		if(obj instanceof IntPair){
			IntPair ipa = (IntPair)obj;
			return ipa.first==first && ipa.second==second;
		}else{
			return false;
		}
		
	}

}
