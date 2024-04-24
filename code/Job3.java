package code;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Job3 {
	// MAPPER 1
	public static class CalculateTFIDFMapper extends MapReduceBase implements Mapper<MyCompositeKey, MyCompositeKey, Text, MyCompositeKey> {
	    private Text outputKey = new Text();
	    private final static IntWritable one = new IntWritable(1);
	    private MyCompositeKey outputVal = new MyCompositeKey();
	    public void map(MyCompositeKey key, MyCompositeKey value, OutputCollector<Text, MyCompositeKey> output, Reporter reporter) throws IOException {
		outputKey.set(key.getLetter());
		outputVal.set(key.getFilename(),one.toString());
		output.collect(outputKey, outputVal); 
	        
	    }
	}
	// REDUCER 1
	public static class CalculateTFIDFReducer extends MapReduceBase implements Reducer<Text, MyCompositeKey, MyCompositeKey, IntWritable> {
	    private MyCompositeKey outputKey = new MyCompositeKey();
	    public void reduce(Text key, Iterator<MyCompositeKey> values, OutputCollector<MyCompositeKey, IntWritable> output, Reporter reporter) throws IOException {
	      
	      int sum = 0;
	      while (values.hasNext()) {
		MyCompositeKey currentKey = values.next();
		sum += Integer.parseInt(currentKey.getFilename());
		outputKey.set(key.toString(),currentKey.getLetter());
	      }
	      output.collect(outputKey, new IntWritable(sum)); 
	 
	    }
	 }
}
