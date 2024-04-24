package code;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Job2 {
	// mapper 2
	public static class NoOfWordsInDocMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, MyCompositeKey> {
	    private Text outputKey = new Text();
	    private MyCompositeKey outputVal = new MyCompositeKey();

	    public void map(LongWritable key, Text value, OutputCollector<Text, MyCompositeKey> output, Reporter reporter) throws IOException {
	      String line = value.toString();
	      StringTokenizer tokenizer = new StringTokenizer(line);
	      while (tokenizer.hasMoreTokens()) {
		    String docname = tokenizer.nextToken(); // 1st token
		    String word = tokenizer.nextToken(); // 2nd token
            int n = Integer.parseInt(tokenizer.nextToken()); // 3rd token
            outputKey.set(docname);
            outputVal.set(word, String.valueOf(n));
            output.collect(outputKey, outputVal);
	      }      
	    }
	}
	// reducer 2
	public static class NoOfWordsInDocReducer extends MapReduceBase implements Reducer<Text, MyCompositeKey, MyCompositeKey, MyCompositeKey> {
	    private MyCompositeKey outputKey = new MyCompositeKey();
            private MyCompositeKey outputVal = new MyCompositeKey();
	    public void reduce(Text key, Iterator<MyCompositeKey> values, OutputCollector<MyCompositeKey, MyCompositeKey> output, Reporter reporter) throws IOException {
	      List <MyCompositeKey> valueList = new ArrayList<>();
	      int sum = 0;
	      while (values.hasNext()) {
            MyCompositeKey currentKey = values.next();
            int wordFreq = Integer.parseInt(currentKey.getFilename()); // n
            sum += wordFreq; // N 
            valueList.add(new MyCompositeKey(currentKey.getLetter(),currentKey.getFilename()));
	      }
	      for(MyCompositeKey currentKey : valueList) {
            String word = currentKey.getLetter();
            int wordFreq = Integer.parseInt(currentKey.getFilename()); // n
            outputKey.set(word, key.toString());
            outputVal.set(String.valueOf(wordFreq), String.valueOf(sum));
            output.collect(outputKey, outputVal);
          }
	    }
	 }
}