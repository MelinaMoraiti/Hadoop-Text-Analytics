package code;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class Job1 {
	// Mapper 1
	// - Input: (docname, contents)
	// - Output: ((word, docname), 1)
	public static class WordFrequencyInDocMapper extends MapReduceBase implements Mapper<LongWritable, Text, MyCompositeKey, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private MyCompositeKey mck = new MyCompositeKey();

	    public void map(LongWritable key, Text value, OutputCollector<MyCompositeKey, IntWritable> output, Reporter reporter) throws IOException {
	      String line = value.toString();
		  //REmove special characters and punctuation
	      line = line.replaceAll("[^a-zA-Z0-9\\s]",""); // remove all non-alphanumeric characters except whitesaces
	      String filename = ((FileSplit) reporter.getInputSplit()).getPath().getName();
	      
	      StringTokenizer tokenizer = new StringTokenizer(line);
	      while (tokenizer.hasMoreTokens()) {
            mck.setComponents(Arrays.asList(tokenizer.nextToken(), filename));
            output.collect(mck, one);
	      }
	    }
	}
	// reducer 1
	public static class WordFrequencyInDocReducer extends MapReduceBase implements Reducer<MyCompositeKey, IntWritable, MyCompositeKey, IntWritable> {
	    public void reduce(MyCompositeKey key, Iterator<IntWritable> values, OutputCollector<MyCompositeKey, IntWritable> output, Reporter reporter) throws IOException {
	      int sum = 0;
	      while (values.hasNext()) {
            sum += values.next().get();
	      }
	      output.collect(key, new IntWritable(sum));
	    }
	 }
}