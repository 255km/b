import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogDriver {

	public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Text word = new Text();
			String valueString = value.toString();
			String[] SingleCountryData = valueString.split("-");
			// word=new Text(SingleCountryData[0]);
			context.write(new Text(SingleCountryData[0]), one);
			// context.write(word, one);

		}
	}

	public static class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text t_key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Text key = t_key;
			int frequencyForCountry = 0;
			for (IntWritable val : values) {
				// replace type of value with the actual type of our value
				frequencyForCountry += val.get();

			}
			// output.collect(key, new IntWritable(frequencyForCountry));
			context.write(key, new IntWritable(frequencyForCountry));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "LogDriver");
		job.setJarByClass(LogDriver.class);
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
