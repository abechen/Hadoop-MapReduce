package com.abe.hadoop_mr_dev;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();
        String year = input.substring(0, 4);
        int temperature = Integer.parseInt(input.substring(11, 15));
        context.write(new Text(year), new IntWritable(temperature));
	}
}
