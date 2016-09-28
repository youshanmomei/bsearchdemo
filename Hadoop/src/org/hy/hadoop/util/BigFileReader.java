package org.hy.hadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class BigFileReader {
	
	
	public static void read(String url, int sline, int eline) throws IOException{
//		BufferedReader br = new BufferedReader(new FileReader(new File(url)));
		BufferedReader br = new BufferedReader(new FileReader(url));
		int linenum = 1;
		String line = null;
		
		while((line=br.readLine())!=null){
			if(linenum>sline && linenum<eline){
				System.out.println(line);
			}else if(linenum==eline){
				System.out.println(line);
				break;
			}
			
			linenum++;
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		String url = "E:/part-r-00000.txt";
		read(url, 11003, 22100);
	}

}
