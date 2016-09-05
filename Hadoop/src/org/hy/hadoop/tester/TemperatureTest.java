package org.hy.hadoop.tester;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.hy.hadoop.test.Temperature;
import org.hy.hadoop.test.Temperature.TemperatureMapper;
import org.hy.hadoop.test.Temperature.TemperatureReducer;
import org.junit.Before;
import org.junit.Test;

/**
 * map 和  reducer集成测试
 * @author andy
 *
 */
public class TemperatureTest {
	
	
	private TemperatureMapper mapper;
	private TemperatureReducer reducer;
	private MapReduceDriver driver;

	@Before
	public void init(){
		mapper = new Temperature.TemperatureMapper();
		reducer = new Temperature.TemperatureReducer();
		driver = new MapReduceDriver<>(mapper, reducer);
	}
	
	@Test
	public void temperatureTest() throws Exception {
		//输入两行测试数据
		String line1 = "1985 07 31 02   200    94 10137   220    26     1     0 -9999";
		String line2 = "1985 07 31 11   100    56 -9999    50     5 -9999     0 -9999";
		
		driver.withInput(new LongWritable(), new Text(line1))
			  .withInput(new LongWritable(), new Text(line2))
			  .withOutput(new Text("weatherStationId"), new IntWritable(150))
			  .runTest();
		
	}
}
