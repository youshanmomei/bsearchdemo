package org.hy.hadoop.mission4.tvplay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TvPlay implements WritableComparable<Object>{
	
	private float playcount; // 播放量
	private float save; // 收藏量
	private float comment; // 评论量
	private float bad; // 踩数
	private float good; // 赞数
	
	public TvPlay() {}
	
	public TvPlay(float playcount, float save, float comment, float bad, float good) {
		this.playcount = playcount;
		this.save = save;
		this.comment = comment;
		this.bad = bad;
		this.good = good;
	}
	
	public void set(float playcount, float save, float comment, float bad, float good) {
		this.playcount = playcount;
		this.save = save;
		this.comment = comment;
		this.bad = bad;
		this.good = good;
	}

	public float getPlaycount() {
		return playcount;
	}

	public void setPlaycount(float playcount) {
		this.playcount = playcount;
	}

	public float getSave() {
		return save;
	}

	public void setSave(float save) {
		this.save = save;
	}

	public float getComment() {
		return comment;
	}

	public void setComment(float comment) {
		this.comment = comment;
	}

	public float getBad() {
		return bad;
	}

	public void setBad(float bad) {
		this.bad = bad;
	}

	public float getGood() {
		return good;
	}

	public void setGood(float good) {
		this.good = good;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(playcount);
		out.writeFloat(save);
		out.writeFloat(comment);
		out.writeFloat(bad);
		out.writeFloat(good);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		playcount = in.readFloat();
		save = in.readFloat();
		comment = in.readFloat();
		bad = in.readFloat();
		good = in.readFloat();
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}
	
}
