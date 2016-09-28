package org.hy.hadoop.mission11.tvratings;

import java.util.ArrayList;
import java.util.List;
/**
 * 时间工具
 * @author andy
 *
 */
public class TimeUtil {

	/**
	 * @function 提取start~end之间的分钟数
	 * <br/><br/>
	 * in: "23:56:45", "24:00:00"<br/>
	 * out: [86160, 86220, 23:56]	[初始时间对应于当天凌晨过去的秒数，当前时间对应于当天凌晨过去的秒数, 当前时间]
	 * @param time
	 * @return list
	 */
	public static List<String[]> getTimeSplit(String start, String end) {
		List<String[]> list = new ArrayList<String[]>();
		String[] s = start.split(":");
		int sh = Integer.parseInt(s[0]);
		int sm = Integer.parseInt(s[1]);
		String[] e = end.split(":");
		int eh = Integer.parseInt(e[0]);
		int em = Integer.parseInt(e[1]);
		if (eh < sh) {
			eh = 24;
		}
		if (sh == eh) {
			for (int m = sm; m <= em; m++) {
				int am = m + 1;
				int ah = sh;
				if (am == 60) {
					am = 0;
					ah += 1;
				}
				String hstr = "";
				String mstr = "";
				if (sh < 10) {
					hstr = "0" + sh;
				} else {
					hstr = sh + "";
				}
				if (m < 10) {
					mstr = "0" + m;
				} else {
					mstr = m + "";
				}
//				String[] time = { sh * 3600 + m * 60 + "", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
				String[] time = { TimeToSecond(start)+"", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
				list.add(time);
			}
		} else {
			for (int h = sh; h <= eh; h++) {
				if (h == 24) {
					break;
				}
				if (h == sh) {
					for (int m = sm; m <= 59; m++) {
						int am = m + 1;
						int ah = h;
						if (am == 60) {
							am = 0;
							ah += 1;
						}
						String hstr = "";
						String mstr = "";
						if (h < 10) {
							hstr = "0" + h;
						} else {
							hstr = h + "";
						}
						if (m < 10) {
							mstr = "0" + m;
						} else {
							mstr = m + "";
						}
//						String[] time = { h * 3600 + m * 60 + "", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						String[] time = { TimeToSecond(start)+"", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						list.add(time);
					}
				} else if (h == eh) {
					for (int m = 0; m <= em; m++) {
						int am = m + 1;
						int ah = h;
						if (am == 60) {
							am = 0;
							ah += 1;
						}
						String hstr = "";
						String mstr = "";
						if (h < 10) {
							hstr = "0" + h;
						} else {
							hstr = h + "";
						}
						if (m < 10) {
							mstr = "0" + m;
						} else {
							mstr = m + "";
						}
//						String[] time = { h * 3600 + m * 60 + "", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						String[] time = { TimeToSecond(start)+"", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						list.add(time);
					}
				} else {
					for (int m = 0; m <= 59; m++) {
						int am = m + 1;
						int ah = h;
						if (am == 60) {
							am = 0;
							ah += 1;
						}
						String hstr = "";
						String mstr = "";
						if (h < 10) {
							hstr = "0" + h;
						} else {
							hstr = h + "";
						}
						if (m < 10) {
							mstr = "0" + m;
						} else {
							mstr = m + "";
						}
//						String[] time = { h * 3600 + m * 60 + "", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						String[] time = { TimeToSecond(start)+"", ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						list.add(time);
					}
				}
			}
		}
		return list;
	}
	
	/**
	 * 将时间00:00:00 转换为秒  int
	 * @param time
	 * @return
	 */
	public static int TimeToSecond(String time){
		if(time==null || time.equals("")) return 0;
		
		String[] ts = time.split(":");
		int hour = Integer.parseInt(ts[0]);
		int min = Integer.parseInt(ts[1]);
		int sec = Integer.parseInt(ts[2]);
		int totalSec = 3600*hour + 60*min + sec;
		return totalSec;
	}
	
	
//	public static void main(String[] args) {
//		List<String[]> list = getTimeSplit("23:56:45", "24:00:00");
//		for (String[] str : list) {
//			System.out.println("["+str[0]+","+str[1]+","+str[2]+"]");
//		}
//	}
}
