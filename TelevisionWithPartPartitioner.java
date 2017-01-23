package televisionwithPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TelevisionWithPartPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
				
		if(key.toString().toUpperCase().charAt(0) >= 'A' && key.charAt(0) <= 'F'){
			return 0;
		}
		else if(key.toString().toUpperCase().charAt(0) >= 'G' && key.charAt(0) <= 'L'){
			return 1;
		}
		else if(key.toString().toUpperCase().charAt(0) >= 'M' && key.charAt(0) <= 'R'){
			return 2;
		}
		else{
			return 3;
		}
			
	}
}