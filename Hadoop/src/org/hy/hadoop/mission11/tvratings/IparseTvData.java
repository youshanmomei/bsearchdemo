package org.hy.hadoop.mission11.tvratings;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * 解析原始数据
 * @author andy
 *
 */
public class IparseTvData {
	
	public static List<String> transData(String text){
		List<String> list = new ArrayList<String>();
		Document doc;
		String rec="";
		
		try{
			doc = Jsoup.parse(text);//解析数据
			Elements content = doc.getElementsByTag("WIC");
			String num = content.get(0).attr("cardNum");//记录编号
			if(num==null || num.equals("")){
				num=" ";
			}
			
			String stbNum = content.get(0).attr("stbNum");//机顶盒编号
			if(stbNum==null || stbNum.equals("")){
				return list;
			}
			
			String date = content.get(0).attr("date");
			if(date==null || date.equals("")){
				return list;
			}
			
			Elements els = doc.getElementsByTag("A");
			if(els.isEmpty()){
				return list;
			}
			
			for (Element el : els) {
				String e = el.attr("e");//end time
				String s = el.attr("s");//start time
				String sn = el.attr("sn");//频道 name
				
				String p = el.attr("p");//节目内容
				p = URLDecoder.decode(p, "utf-8");//对节目解码
				//解析出统一的节目名称
				//天龙八部(1)、天龙八部(2)属于同一个节目
				int index = p.indexOf("(");
				if(index!=-1) p=p.substring(0, index);
				
				rec = stbNum + "@" + date + "@" + sn + "@" + s + "@" + e + "@" + p;
				list.add(rec);
			}
			
		}catch(Exception e){
			System.out.println(e.getMessage());
			return list;
		}
		
		return list;
	}
}
