package com.pactera.hadoop.lessen01;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMap extends Mapper<LongWritable, Text, Text, Text> {

	
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] words = line.split("\\s");
		
		for(int i=0;i<words.length;i++) {
			String date=words[0];
			String time=words[1];
			String temperature=words[2];
			
			String vOut=time+" "+temperature;
			
			context.write(new Text(date), new Text(vOut));
		}
	}
	
}
