package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class filewcount {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, MyCompositeKey, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private MyCompositeKey mck = new MyCompositeKey();

    public void map(LongWritable key, Text value, OutputCollector<MyCompositeKey, IntWritable> output, Reporter reporter) throws IOException {
      String filename = ((FileSplit) reporter.getInputSplit()).getPath().getName();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
          mck.set(tokenizer.nextToken(), filename);
          output.collect(mck, one);
      }
    }
  }


  public static class Reduce extends MapReduceBase implements Reducer<MyCompositeKey, IntWritable, MyCompositeKey, IntWritable> {
    public void reduce(MyCompositeKey key, Iterator<IntWritable> values, OutputCollector<MyCompositeKey, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(filewcount.class);
    conf.setJobName("filewcount");

    conf.setOutputKeyClass(MyCompositeKey.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
