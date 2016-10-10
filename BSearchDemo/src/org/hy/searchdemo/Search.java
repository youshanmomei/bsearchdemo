package org.hy.searchdemo;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Search extends HttpServlet{
	//deal with GET request from the client
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		//这条语句指明了向客户端发送的内容格式和采用的字符编码
		resp.setContentType("text/heml;charset=UTF-8");
		String wd = req.getParameter("wd").trim();
		wd = new String(wd.getBytes("ISO-8859-1"), "utf-8");
		
		
		//this is searching coding spacing
		//use PrintWrite's Object method to put date to client
		PrintWriter out = resp.getWriter();
		out.print("<font color=red>" + wd + "</font>的检索结果如下：<hr/>");
		out.println("<p>秦羽，玄幻小说《星辰变》中的男主角，秦始皇后人，王爷“秦德”的三世子，由于体内丹田先天不足，决定了无法修炼内功的劣势，但本人不甘没落，从小有着成为强者的决心，和别的小说主角不同，别的小说主角只是单纯的盲目的修炼，但秦羽不同，为了自己心爱的姜立和少年时候遇见的对他说比自己更懂天的男子，强烈的决心造就了他，终于成为了宇宙中独一无二的神—秦蒙。</p>");

		out.println("<h3><a href='http://baike.baidu.com/link?url=xUBeAJH2eoHnyliRHxSCgZda5n56UrofSnL3xyuwNG3sS7R5Wji_yoUw2pMi_e8b5byYMQz33Hi9kU1lmsicSa'>秦羽-《星辰变》</a></h3>");
		out.println("<p>秦羽，玄幻小说《星辰变》中的男主角，秦始皇后人，王爷“秦德”的三世子，由于体内丹田先天不足，决定了无法修炼内功的劣势，但本人不甘没落，从小有着成为强者的决心，和别的小说主角不同，别的小说主角只是单纯的盲目的修炼，但秦羽不同，为了自己心爱的姜立和少年时候遇见的对他说比自己更懂天的男子，强烈的决心造就了他，终于成为了宇宙中独一无二的神—秦蒙。</p>");
		
		out.close();
	}
	
	//deal with POST request from the client
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doGet(req, resp);//when client use POST request, put it to doGet()
	}
	
}
