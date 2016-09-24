package org.hy.hadoop.mission7.tv;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ParseTVData {
	
	/**
	 * parse input data list
	 * return list style data
	 * @param text
	 * @return
	 */
	public static List<String> transData(String text){
		List<String> list = new ArrayList<String>();
		Document doc;
		String rec = "";
		
		try {
			doc = Jsoup.parse(text);//parse html data
			Elements content = doc.getElementsByTag("WIC");
			String num = content.get(0).attr("cardNum");//record num
			if (num==null||num.equals("")) {
				num=" ";
			}
			
			String stbNum = content.get(0).attr("stbNum");//set top box num
			if(stbNum.equals("")){
				return list;
			}
			
			
			String date = content.get(0).attr("data");
			
			Elements els = doc.getElementsByTag("A");
			if (els.isEmpty()) {
				return list;
			}
			
			for(Element el:els){
				String e = el.attr("e");//end time
				String s = el.attr("s");//start time
				String sn = el.attr("sn");//channel name
				
				rec = stbNum + "@" + date + "@" + sn + "@" + s + "@" + e;
				list.add(rec);
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return list;
		}
		
		return list;
	}

}
