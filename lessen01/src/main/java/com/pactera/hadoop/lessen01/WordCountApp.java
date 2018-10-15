package com.pactera.hadoop.lessen01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountApp {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration con = new Configuration();
		Job job = Job.getInstance(con,"firstLessen01");
		job.setJarByClass(WordCountApp.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		job.setMapperClass(WordCountMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(WordCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
