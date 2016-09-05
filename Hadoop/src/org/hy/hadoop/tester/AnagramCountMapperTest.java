package org.hy.hadoop.tester;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.hy.hadoop.test.AnagramCount;
import org.hy.hadoop.test.AnagramCount.AnagramCountMapper;
import org.junit.Before;
import org.junit.Test;

public class AnagramCountMapperTest {
	
	
	private AnagramCountMapper mapper;
	private MapDriver<LongWritable, Text, Text, Text> driver;

	@Before
	public void init() {
		mapper = new AnagramCount.AnagramCountMapper();
		driver = new MapDriver<>(mapper);
	}
	
	@Test
	public void anagramCountMapperTest() throws Exception {
		String line = "hadoop";
		driver.withInput(new LongWritable(), new Text(line))
			  .withOutput(new Text("adhoop"), new Text("hadoop"))
			  .runTest();
		
	}

}
