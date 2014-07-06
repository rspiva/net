package br.com.net.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NetCgnatReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
		Text temp = new Text();
		String start_ts = "", end_en = "";
		
		for (Text val : values) {
			String[] valueTokens = val.toString().split(",");
		    start_ts = valueTokens[0];
		    end_en	= valueTokens[1];		      
		}
		
		String[] rawTokens = key.toString().split(",");
	    //firstkey is starting time, lastEntry.getValue has last end time
	    temp.set(start_ts + "\t|" + end_en + "\t|" + rawTokens[0] + "\t|" + rawTokens[1] + "\t|" + rawTokens[2] + "\t|" + rawTokens[3] + "\t|" + rawTokens[4]);
	    context.write(NullWritable.get(), temp);
		
	}
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
					throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
