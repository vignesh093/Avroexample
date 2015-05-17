package com.iv.avro;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Avroreducer extends Reducer<Text,FloatWritable,AvroKey<String>,AvroValue<Float>>{
	
	public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException,InterruptedException
	{
		float f=0;
		for(FloatWritable val:value)
		{
			f+=val.get();
		}
		context.write(new AvroKey<String>(key.toString()),new AvroValue<Float>(f));
	}

}
