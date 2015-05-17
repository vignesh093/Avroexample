package com.iv.avro;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.iv.avro.weatherdata;


public class Avromapper extends Mapper<AvroKey<weatherdata>,NullWritable,Text,FloatWritable>{
public void mapper(AvroKey<weatherdata> key,NullWritable value,Context context) throws IOException,InterruptedException
{
	String s=(String) key.datum().getPlace();
	Float s1=key.datum().getTemp();
	context.write(new Text(s),new FloatWritable(s1));
}
}
