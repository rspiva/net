package br.com.net.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NetCgnatMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private static String SEPERATOR = ",";
	Text key = new Text();
	Text val = new Text();
	
	@Override
	protected void map(LongWritable k, Text text, Context context)
                 		  throws IOException, InterruptedException {
		
		
		
		
	}

}
