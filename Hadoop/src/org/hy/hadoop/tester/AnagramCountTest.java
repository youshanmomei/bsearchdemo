package org.hy.hadoop.tester;

import static org.junit.Assert.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.hy.hadoop.test.AnagramCount;
import org.hy.hadoop.test.AnagramCount.AnagramCountMapper;
import org.hy.hadoop.test.AnagramCount.AnagramCountReducer;
import org.junit.Before;
import org.junit.Test;

public class AnagramCountTest {
	
	private AnagramCountMapper mapper;
	private AnagramCountReducer reduce;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> driver;

	@Before
	public void init(){
		mapper = new AnagramCount.AnagramCountMapper();
		reduce = new AnagramCount.AnagramCountReducer();
		driver = new MapReduceDriver<>(mapper, reduce);
	}
	
	@Test
	public void anagramCountTest() throws Exception {
		String line1 = "cosher";
		String line2 = "chores";
		String line3 = "ochres";
		
		driver.withInput(new LongWritable(),new Text(line1))
			  .withInput(new LongWritable(), new Text(line2))
			  .withInput(new LongWritable(), new Text(line3))
			  .withOutput(new Text("cehors"), new Text("cosher,chores,ochres"))
			  .runTest();
	}

}
