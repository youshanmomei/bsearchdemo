package org.hy.hadoop.tester;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.hy.hadoop.test.AnagramCount;
import org.hy.hadoop.test.AnagramCount.AnagramCountReducer;
import org.junit.Before;
import org.junit.Test;

public class AngaramCountReducerTest {
	
	private AnagramCountReducer reducer;
	private ReduceDriver<Text, Text, Text, Text> driver;

	@Before
	public void init(){
		reducer = new AnagramCount.AnagramCountReducer();
		driver = new ReduceDriver(reducer);
	}
	
	@Test
	public void angaramCountReducerTest() throws Exception {
		String key = "adhoop";
		List values = new ArrayList<>();
		values.add(new Text("hadoop"));
		values.add(new Text("oohadp"));
		driver.withInput(new Text(key), values)
			  .withOutput(new Text(key), new Text("hadoop,oohadp"))
			  .runTest();
		
	}

}
