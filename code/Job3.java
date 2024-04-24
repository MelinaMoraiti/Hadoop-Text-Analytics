package code;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.w3c.dom.Text;

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
          double maxTf = Double.MIN_VALUE;
          String maxDocname = "";
	      while (values.hasNext()) {
            MyCompositeKey currentKey = values.next();
            String currentDocname = currentKey.getComponents().get(0);
            double tf = Double.parseDouble(currentKey.getComponents().get(1));
            String ones  = currentKey.getComponents().get(2);
            noOfDocsWordExists += Integer.parseInt(ones);
            if (tf > maxTf)
            {
                maxTf = tf;
                maxDocname = currentDocname;
            }
            //double tfidf = Double.parseDouble(tf) * Math.log((double) totalDocs/ noOfDocsWordExists);
	      }	 
          String word = key.toString();
          outputVal.setComponents(Arrays.asList(String.valueOf(maxTf),String.valueOf(noOfDocsWordExists)));
          outputKey.setComponents(Arrays.asList(word,maxDocname));
          output.collect(outputKey, outputVal); 
	    }
	 }
}
