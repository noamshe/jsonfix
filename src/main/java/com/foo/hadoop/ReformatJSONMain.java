package com.foo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: noamshe
 * Date: 24-06-14
 */
public class ReformatJSONMain {

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    conf = new Configuration();
//    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/core-site.xml"));
//    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
//    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/mapred-site.xml"));
    Job job = new Job(conf, "ReformatJSONMain");
    job.setJarByClass(ReformatJSONMain.class);
    job.setMapperClass(JsonReFormatMapper.class);
//    job.setReducerClass(NotUsingReducer.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
//    String hdfsInput = "/dearnoam";
//    String hdfsOutput = "/thisistheoutput";
    String hdfsInput = args[0];
    String hdfsOutput = args[1];
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.addInputPath(job, new Path(hdfsInput));
    FileOutputFormat.setOutputPath(job, new Path(hdfsOutput));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
