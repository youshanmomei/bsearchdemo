package org.hy.hadoop.mission10.television;

import java.util.ArrayList;
import java.util.List;

public class ParseTime {

	/**
	 * @function 提取start~end之间的分钟数
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
				String[] time = { sh * 3600 + m * 60 + "",
						ah * 3600 + am * 60 + "", hstr + ":" + mstr };
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
						String[] time = { h * 3600 + m * 60 + "",
								ah * 3600 + am * 60 + "", hstr + ":" + mstr };
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
						String[] time = { h * 3600 + m * 60 + "",
								ah * 3600 + am * 60 + "", hstr + ":" + mstr };
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
						String[] time = { h * 3600 + m * 60 + "",
								ah * 3600 + am * 60 + "", hstr + ":" + mstr };
						list.add(time);
					}
				}
			}
		}
		return list;
	}
}
