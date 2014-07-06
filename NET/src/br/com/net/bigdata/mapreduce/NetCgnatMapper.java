package br.com.net.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NetCgnatMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private static String SEPERATOR = ",";
	Text key = new Text();
	Text val = new Text();
	
	@Override
	protected void map(LongWritable k, Text text, Context context)
                 		  throws IOException, InterruptedException {
		
		//Levantar nome do arquivo
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		//Nome do Arquivo: cps_20140216060000.txt
		String[] tokens = text.toString().split(SEPERATOR);
		String start_ts 		= tokens[1];
		String end_ts			= tokens[2];
		String origem_ip 		= tokens[3];
		String origem_port		= tokens[4];
		String translate_ip		= tokens[5];
		String translate_port	= tokens[6]; 
		
		key.set(origem_ip + "," + origem_port + "," + translate_ip	+ "," + translate_port + "," + fileName);
		val.set(start_ts + "," + end_ts);
	    context.write(key, val);
	}
}