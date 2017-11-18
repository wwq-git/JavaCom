package com.go.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobClient {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String host = "192.168.99.100";
		System.out.println("hdfs://" + host + ":9000");
		System.exit(1);
		Configuration conf = new Configuration();
		conf.set("yarn.resourcemanager.hostname", host);
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobClient.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(2);
		String HDFSPath = "hdfs://" + host + ":9000";
		FileInputFormat.setInputPaths(job, new Path(HDFSPath + "/datasrc/input/"));
		FileOutputFormat.setOutputPath(job, new Path(HDFSPath + "/datasrc/output/"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}

}
