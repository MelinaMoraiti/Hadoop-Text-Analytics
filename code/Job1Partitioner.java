package code;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.IntWritable;
import code.MyCompositeKey;
import org.apache.hadoop.mapred.JobConf;

public class Job1Partitioner implements Partitioner <MyCompositeKey, IntWritable> 
{
        @Override
	public void configure(JobConf job) {
	    // No configuration needed for this partitioner
	}
	@Override
	public int getPartition(MyCompositeKey key, IntWritable value, int numPartitions)
	{
	    String word = key.getComponents().get(0);
	    char firstChar = word.charAt(0);
	    return (int) firstChar % numPartitions;
	}

}