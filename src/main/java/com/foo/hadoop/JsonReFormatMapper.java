package com.foo.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 * Date: 16-07-13
 * Time: 12:07
 */
public class JsonReFormatMapper extends Mapper<LongWritable,Text,Text,Text> {

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    String singleRequest = value.toString();

    String result1 = singleRequest.replace("bid_request\":\"", "bid_request\":");
    String result2 = result1.replace("\",\"time_stamp", ",\"time_stamp");

    context.write(null, new Text(result2));
  }
}
