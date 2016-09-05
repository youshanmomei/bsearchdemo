package org.hy.hadoop.tester;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.hy.hadoop.test.Temperature;
import org.hy.hadoop.test.Temperature.TemperatureReducer;
import org.junit.Before;
import org.junit.Test;

public class TemperatureReducerTest {
	private Reducer reducer;
	private ReduceDriver driver;
	
	@Before
	public void init(){
		//new instance
		reducer = new Temperature.TemperatureReducer();
		driver = new ReduceDriver<>();
	}
	
	@Test
	public void temReducerTest() throws Exception {
		String key = "weatherStationId";
		List values = new ArrayList();
		values.add(new IntWritable(100));
		values.add(new IntWritable(200));
		driver.withInput(new Text("weatherStationId"), values)
			  .withOutput(new Text("weatherStationId"), new IntWritable(150))
			  .runTest();
	}
	
	

}
