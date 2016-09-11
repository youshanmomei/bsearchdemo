package org.hy.hadoop.scorecount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 学习成绩读写类 data style: 19020090017 小讲 90 99 100 89 95
 * 
 * @author andy
 * 
 */
public class ScoreWritable implements WritableComparable<Object>{
	private float chinese;
	private float math;
	private float english;
	private float physics;
	private float chemistry;

	public ScoreWritable() {
		super();
	}

	public ScoreWritable(float chinese, float math, float english,
			float physics, float chemistry) {
		super();
		this.chinese = chinese;
		this.math = math;
		this.english = english;
		this.physics = physics;
		this.chemistry = chemistry;
	}

	public void set(float chinese, float math, float english, float physics,
			float chemistry) {
		this.chinese = chinese;
		this.math = math;
		this.english = english;
		this.physics = physics;
		this.chemistry = chemistry;
	}

	public float getChinese() {
		return chinese;
	}

	public void setChinese(float chinese) {
		this.chinese = chinese;
	}

	public float getMath() {
		return math;
	}

	public void setMath(float math) {
		this.math = math;
	}

	public float getEnglish() {
		return english;
	}

	public void setEnglish(float english) {
		this.english = english;
	}

	public float getPhysics() {
		return physics;
	}

	public void setPhysics(float physics) {
		this.physics = physics;
	}

	public float getChemistry() {
		return chemistry;
	}

	public void setChemistry(float chemistry) {
		this.chemistry = chemistry;
	}

	@Override
	public String toString() {
		return "ScoreWritable [chinese=" + chinese + ", math=" + math
				+ ", english=" + english + ", physics=" + physics
				+ ", chemistry=" + chemistry + "]";
	}

	/**
	 * 将对象转换为字节流写入到输出流out中
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(chinese);
		out.writeFloat(math);
		out.writeFloat(english);
		out.writeFloat(physics);
		out.writeFloat(chemistry);
	}

	/**
	 * 从输入流in中读取字节流反序列化为对象
	 * in.readFloat();的源码显示这是一个抽象的方法
	 * 根据这一形式，倒是可以猜测，后面是不是调用反射的方法将匹配field的值，然后用get/set的方式自动赋值
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		chinese = in.readFloat();
		math = in.readFloat();
		english = in.readFloat();
		physics = in.readFloat();
		chemistry = in.readFloat();
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}
	
	

}
