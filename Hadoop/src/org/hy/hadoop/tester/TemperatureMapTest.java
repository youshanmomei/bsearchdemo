package org.hy.hadoop.tester;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.hy.hadoop.test.Temperature;
import org.junit.Before;
import org.junit.Test;

public class TemperatureMapTest {
	private Mapper mapper;
	private MapDriver driver;
	
	
	@Before
	public void init() {
		//实例化TemperatureMapper对象
		mapper = new Temperature.TemperatureMapper();
		//实例化MapDriver对象
		driver = new MapDriver<>(mapper);

	}
	
	@Test
	public void tempTest() throws Exception {
		//input a line test data
		String line = "1985 07 31 02   200    94 10137   220    26     1     0 -9999";
		driver.withInput(new LongWritable(), new Text(line))
			  .withOutput(new Text("weatherStationId"), new IntWritable(200))
			  .runTest();
	}

}
