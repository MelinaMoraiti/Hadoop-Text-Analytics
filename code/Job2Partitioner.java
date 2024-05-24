package code;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.Text;
import code.MyCompositeKey;
import org.apache.hadoop.mapred.JobConf;

public class Job2Partitioner implements Partitioner <Text, MyCompositeKey> 
{    
        @Override
	public void configure(JobConf job) {
	    // No configuration needed for this partitioner
	}
	@Override
	public int getPartition(Text key, MyCompositeKey value, int numPartitions)
	{
	    String word = value.getComponents().get(0);
	    char firstChar = word.charAt(0);
	    return (int) firstChar % numPartitions;
	}

}
