package com.iv.avro;


import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvroDriver {
	public static void main(String args[]) throws Exception
	{
		if(args.length!=2)
			{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
			}
		Configuration conf=new Configuration();
		Job job=new Job(conf,"AvroDriver");
		
		job.setJarByClass(AvroDriver.class);
		job.setJobName("AvroDriver");
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		job.setMapperClass(Avromapper.class);
		
		job.setReducerClass(Avroreducer.class);
		AvroJob.setInputKeySchema(job,weatherdata.getClassSchema());
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		AvroJob.setOutputKeySchema(job,Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job,Schema.create(Schema.Type.FLOAT));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
