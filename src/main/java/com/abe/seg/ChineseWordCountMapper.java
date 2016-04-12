package com.abe.seg;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.SegToken;

public class ChineseWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputValue = new IntWritable(1);
	private JiebaSegmenter segmenter = new JiebaSegmenter();
	private Text outputKey = new Text();
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String sentence =  ivalue.toString();
		
		List<SegToken> words = segmenter.process(sentence, SegMode.INDEX);
		for(SegToken result : words){
			if(result.word.length() >= 2){
				outputKey.set(result.word);
				context.write(outputKey, outputValue);
			}
		}
	}
}