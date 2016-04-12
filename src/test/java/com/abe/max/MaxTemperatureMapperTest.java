package com.abe.max;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class MaxTemperatureMapperTest {
	@Test
	public void processesValidRecord() throws IOException, InterruptedException{
		Text value = new Text("1989/07/23 9999");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(0), value)
		.withOutput(new Text("1988"), new IntWritable(9998))
		.runTest();
		
	}
}
