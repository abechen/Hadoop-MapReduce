package com.abe.seg;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChineseWordCountDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ChineseWordCountDriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "ChineseWordCount");
		job.setJarByClass(getClass());
		job.setMapperClass(ChineseWordCountMapper.class);
		job.setReducerClass(ChineseWordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
