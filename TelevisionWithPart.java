package televisionwithPartitioner;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TelevisionWithPart {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoTelevision");
		job.setJarByClass(TelevisionWithPart.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TelevisionWithPartMapper.class);
		job.setReducerClass(TelevisionWithPartReducer.class);
		job.setPartitionerClass(TelevisionWithPartPartitioner.class);
		job.setCombinerClass(TelevisionWithPartCombiner.class);
		
		job.setNumReduceTasks(4);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/acadgild/hadoop/outputtelevision/part-r-00000")); 
		FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/user/acadgild/hadoop/outputtelevision/soldcompanywisewithpartcombs"));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
