package com.pactera.hadoop.lessen01;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, Text, Text, LongWritable> {

	@Override
	protected void reduce(Text k2, Iterable<Text> v2,Reducer<Text, Text, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String maxTemperatureResult=null;
		Long maxTemperature=0l;
		
		for (Text timeTemperature : v2) {
			String[] strs=timeTemperature.toString().split("\\s");
			String time=strs[0];
			String temperatureText=strs[1];
			Long temperature=Long.valueOf(temperatureText);
			
			if(maxTemperatureResult==null || temperature.compareTo(maxTemperature)>0) {
				maxTemperatureResult=k2.toString()+" "+timeTemperature.toString();
				maxTemperature=temperature;
			}
		}
		arg2.write(new Text(maxTemperatureResult), new LongWritable(maxTemperature));
	}
}
