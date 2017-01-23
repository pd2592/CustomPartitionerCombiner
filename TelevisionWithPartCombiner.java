package televisionwithPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TelevisionWithPartCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	IntWritable outValue = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		for (IntWritable value : values) {
				sum += value.get();
			}
		outValue.set(sum);
		context.write(key, outValue);
		}
	
}
