package code;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import code.Job1.WordFrequencyInDocMapper;
import code.Job1.WordFrequencyInDocReducer;
import code.Job2.NoOfWordsInDocMapper;
import code.Job2.NoOfWordsInDocReducer;
public class Main {

    public static void main(String[] args) throws Exception {
        //JOB 1
        JobConf conf = new JobConf(Job1.class);
        conf.setJobName("Job1");

        conf.setOutputKeyClass(MyCompositeKey.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordFrequencyInDocMapper.class);
        conf.setCombinerClass(WordFrequencyInDocReducer.class);
        conf.setReducerClass(WordFrequencyInDocReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        //JOB 2
        JobConf conf2 = new JobConf(Job2.class);
            conf2.setJobName("Job2");

        conf2.setMapOutputKeyClass(Text.class);
        conf2.setOutputKeyClass(MyCompositeKey.class);
        conf2.setOutputValueClass(MyCompositeKey.class);

        conf2.setMapperClass(NoOfWordsInDocMapper.class);
        //conf2.setCombinerClass(NoOfWordsInDocReducer.class);
        conf2.setReducerClass(NoOfWordsInDocReducer.class);
	
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        conf2.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(conf2, new Path(args[1])); // Input is Output of 1st Job 
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

        JobClient.runJob(conf2);

    }
}