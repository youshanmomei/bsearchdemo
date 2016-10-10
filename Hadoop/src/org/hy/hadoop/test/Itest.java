package org.hy.hadoop.test;

import java.util.Date;

public class Itest {

}


class MyThread implements Runnable{

	@Override
	public void run() {
		while(true){
			System.out.println("--------"+new Date()+"------");
//			sleep(1000);
		}
		
	}
	
}