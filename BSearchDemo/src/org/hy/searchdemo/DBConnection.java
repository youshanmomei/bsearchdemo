package org.hy.searchdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;


/**
 * @function 连接mysql数据库，对表进行查询
 * @author andy
 *
 */
public class DBConnection {

	/** 驱动类名称  **/
	private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
	
	/** 数据库连接字符串  **/
	private static final String DATABASE_URL = "jdbc:mysql://192.168.80.128:3306/xxx";
	
	/** 数据库用户名 **/
	private static final String USER_NAME = "root";
	
	/** 数据库密码 **/
	private static final String PASSWORD = "root";
	
	/** 数据库连接类 **/
	private static Connection conn;
	
	/** 数据库操作类 **/
	private static Statement stmt;
	
	
	//load driver
	static {
		try {
			Class.forName(DRIVER_CLASS);
		} catch (Exception e) {
			System.out.println("加载驱动错误");
			e.printStackTrace();
		}
	}
	
	
	//get connection
	private static Connection getConntection(){
		
		try {
			conn = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
		} catch (Exception e) {
			System.out.println("取得连接错误");
			e.printStackTrace();
		}
		
		return conn;
	}
	
	//query the database and return list
	public ArrayList<String> getList(String sql){
		ArrayList<String> list = null;
		
		//取得数据库操作对象
		try {
			stmt = getConntection().createStatement();
		} catch (Exception e) {
			System.out.println("stattemnet取得错误");
			e.printStackTrace();
			return null;
		}
		
		try {
			list = new ArrayList<String>();
			//查询数据库对象，返回记录级集（结果集）
			ResultSet rs = stmt.executeQuery(sql);
			
			//circulation record collection 
			//view the record of each column in a row
			while(rs.next()){
				String keyword = rs.getString(1);
				list.add(keyword);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
}
