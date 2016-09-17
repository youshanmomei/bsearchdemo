package org.hy.hadoop.mission4.tvplay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 电视剧热度数据的bean <br/>
 * <br/>
 * name 网站 播放量 收藏量 评论 踩 赞<br/>
 * 继承者们 1 4105447 202 844 48 671<br/>
 * ... ... <br/>
 * 1 - youku 2 - tudou 3 - aiqiyi 4 - souhu 5 - xunlei
 * 
 * <br/>
 * <br/>
 * 数据bean类只要放int或者float类型的数据，尤其不要放String类型的，无法判别
 * 
 * @author andy
 * @version 1.0
 */
public class TvPlay_old implements WritableComparable<Object> {
	public final static String[] TYPENAME = { "youku", "tudou", "aiqiyi", "souhu", "xunlei" };

	// private String name; //电视剧名称
	private int web; // 网站的类别
	private int playcount; // 播放量
	private int save; // 收藏量
	private int comment; // 评论量
	private int bad; // 踩数
	private int good; // 赞数

	public int getWeb() {
		return web;
	}

	public void setWeb(int web) {
		this.web = web;
	}

	public int getPlaycount() {
		return playcount;
	}

	public void setPlaycount(int playcount) {
		this.playcount = playcount;
	}

	public int getSave() {
		return save;
	}

	public void setSave(int save) {
		this.save = save;
	}

	public int getComment() {
		return comment;
	}

	public void setComment(int comment) {
		this.comment = comment;
	}

	public int getBad() {
		return bad;
	}

	public void setBad(int bad) {
		this.bad = bad;
	}

	public int getGood() {
		return good;
	}

	public void setGood(int good) {
		this.good = good;
	}

	public TvPlay_old() {
	}

	public TvPlay_old(int web, int playcount, int save, int comment, int bad,
			int good) {
		this.web = web;
		this.playcount = playcount;
		this.save = save;
		this.comment = comment;
		this.bad = bad;
		this.good = good;
	}
	
	public void set(int web, int playcount, int save, int comment, int bad,
			int good) {
		this.web = web;
		this.playcount = playcount;
		this.save = save;
		this.comment = comment;
		this.bad = bad;
		this.good = good;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(web);
		out.write(playcount);
		out.write(save);
		out.write(comment);
		out.write(bad);
		out.write(good);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		web = in.readInt();
//		playcount = in.readInt();TYPENAME
//		save = in.readInt();
//		comment = in.readInt();
//		bad = in.readInt();
//		good = in.readInt();
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}

}
