import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ApacheLogInMapperAverage {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(ApacheLogInCombineMapper.class);
		job.setReducerClass(ApacheLogInCombineReducer.class);
		
		job.setJarByClass(WordCount.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
