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
            outputVal.setComponents(Arrays.asList(word, String.valueOf(n)));
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
                int noOfWordsInDoc = 0;
                while (values.hasNext()) {
                    MyCompositeKey currentKey = values.next();
                    int wordFreq = Integer.parseInt(currentKey.getComponents().get(1)); // n
                    noOfWordsInDoc += wordFreq; // total number of words in file (N) 
                    String word = currentKey.getComponents().get(0);
                    valueList.add(new MyCompositeKey(word,String.valueOf(wordFreq)));
                }
                for(MyCompositeKey currentKey : valueList) {
                    String word = currentKey.getComponents().get(0);
                    int wordFreq =  Integer.parseInt(currentKey.getComponents().get(1)); // n
                    outputKey.setComponents(Arrays.asList(word, key.toString()));
                    outputVal.setComponents(Arrays.asList(String.valueOf(wordFreq), String.valueOf(noOfWordsInDoc)));
                    output.collect(outputKey, outputVal);
                }
            }
	 }
}