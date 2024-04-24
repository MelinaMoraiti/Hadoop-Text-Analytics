package code;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Job3 {
	// MAPPER 3
	public static class CalculateTFIDFMapper extends MapReduceBase implements Mapper<MyCompositeKey, MyCompositeKey, Text, MyCompositeKey> {
	    private Text outputKey = new Text();
	    private final static IntWritable one = new IntWritable(1);
	    private MyCompositeKey outputVal = new MyCompositeKey();
	    public void map(MyCompositeKey key, MyCompositeKey value, OutputCollector<Text, MyCompositeKey> output, Reporter reporter) throws IOException {
            String docname = key.getComponents().get(1);
            String word = key.getComponents().get(0);
            String wordFreq = value.getComponents().get(0);
            String noOfWords = value.getComponents().get(1);
            double tf = (double) Integer.parseInt(wordFreq)/Integer.parseInt(noOfWords);
            outputKey.set(word);
		    outputVal.setComponents(Arrays.asList(docname,String.valueOf(tf),one.toString()));
		    output.collect(outputKey, outputVal);    
	    }
	}
	// REDUCER 3
	public static class CalculateTFIDFReducer extends MapReduceBase implements Reducer<Text, MyCompositeKey, MyCompositeKey, MyCompositeKey> {
        private MyCompositeKey outputKey = new MyCompositeKey();
	    private MyCompositeKey outputVal = new MyCompositeKey();
        //private int totalDocs = 3;
	    public void reduce(Text key, Iterator<MyCompositeKey> values, OutputCollector<MyCompositeKey, MyCompositeKey> output, Reporter reporter) throws IOException {
          int noOfDocsWordExists = 0;
	      while (values.hasNext()) {
            MyCompositeKey currentKey = values.next();
            String docname = currentKey.getComponents().get(0);
            String tf = currentKey.getComponents().get(1);
            String ones  = currentKey.getComponents().get(2);
            String word = key.toString();
            noOfDocsWordExists += Integer.parseInt(ones);
            //double tfidf = Double.parseDouble(tf) * Math.log((double) totalDocs/ noOfDocsWordExists);
            outputVal.setComponents(Arrays.asList(String.valueOf(tf),String.valueOf(noOfDocsWordExists)));
            outputKey.setComponents(Arrays.asList(word,docname));
            output.collect(outputKey, outputVal); 
	      }	 
	    }
	 }
}
