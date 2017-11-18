package com.go.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		String line =value.toString();
		String [] lineWords=line.split(" ");
		for (String word : lineWords) {
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
