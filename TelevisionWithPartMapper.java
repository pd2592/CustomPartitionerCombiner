package televisionwithPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.amazonaws.services.simpleworkflow.flow.worker.SynchronousActivityTaskPoller; 

public class TelevisionWithPartMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	Text outkey = new Text();
	IntWritable outvalue = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		//String state;	
		String company = value.toString().split("\\|")[0];
		outkey.set(company);
			context.write(outkey, outvalue);
			
			
		}
	}
