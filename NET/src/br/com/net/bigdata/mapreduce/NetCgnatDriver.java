package br.com.net.bigdata.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NetCgnatDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(NetCgnatDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,true);

		job.setMapperClass(NetCgnatMapper.class);
		job.setReducerClass(NetCgnatReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: " + "NetCgnatDriver"
					+ "<in> <out>");
			System.exit(2);
		}
		int exitCode = ToolRunner.run(new NetCgnatDriver(),
				args);
		System.exit(exitCode);
	}

}
