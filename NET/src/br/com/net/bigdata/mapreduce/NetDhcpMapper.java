package br.com.net.bigdata.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NetDhcpMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static String SEPERATOR = ",";
	Text key = new Text();
	Text val = new Text();
	
	 //#Sun Feb 16 04:06:00 2014
    SimpleDateFormat sdf1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	protected void map(LongWritable k, Text text, Context context)
                 		  throws IOException, InterruptedException {
		String rawTokens = text.toString();
	    String[] tokens = rawTokens.replaceAll("\"", "").split(SEPERATOR);
	    
	    try {
	        Date st_time = sdf1.parse(tokens[0]);
	        Date en_time = sdf1.parse(tokens[1]);

	        String start_time = sdf2.format(st_time);
	        String end_time = sdf2.format(en_time);
	        String ip_address = tokens[2];
	        String mac_address = tokens[4];
	        String action = tokens[6];

	        key.set(mac_address + "," + ip_address + "," + action);
	        val.set(start_time + "," + end_time);
	        context.write(key, val);
	      } catch (ParseException e) {
	        e.printStackTrace();
	      }
	}		
}
