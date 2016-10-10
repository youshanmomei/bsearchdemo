package org.hy.searchdemo;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONArray;

public class Suggest extends HttpServlet{
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		resp.setContentType("text/html;charset=UTF-8");
		//get the keyword 'term' from the front 
		String term = req.getParameter("term").trim();
		//set the character set encoding of 'term'
		term = new String(term.getBytes("ISO-8859-1"), "utf-8");
		//use PrintWriter's method to send the data to client
		PrintWriter out = resp.getWriter();
		
		if(!term.isEmpty()){
			DBConnection db = new DBConnection();
			String sql = "select keyword from dictionary where keyword like %'" + term + "' order by frequncy desc limit 0,5";
			
			ArrayList<String> list = db.getList(sql);
			if(list==null){
				list = new ArrayList<String>();
				list.add(term);
			}
			
			//return json format of the data
			out.println(JSONArray.fromObject(list).toString());
		}else{
			out.println("");
		}
		out.close();
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doGet(req, resp);
	}

}
