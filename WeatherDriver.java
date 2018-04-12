import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherDriver {

	public static class WeatherMapper extends Mapper<Object,Text,Text,IntWritable>
	{
		public Text word = new Text();
		public void map(Object key,Text value,Context context) throws InterruptedException,IOException
		{
			String s = value.toString();
			String splitfirst = "\n";
			String []b = s.split(splitfirst);
			for(String b1:b)
			{
				String year = b1.substring(15, 19);
				String temp = b1.substring(87, 92);
				int temp1 = Integer.parseInt(temp);
				IntWritable one = new IntWritable(temp1); 
				word.set(year);
				context.write(word, one);
			}
		}
	}
	

public static class WeatherReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	IntWritable result = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
			int max = 0;
			for(IntWritable val:values)
			{
				int temp = (val.get());
				if(max < temp)
				{
					max = temp;
				}
			}
			result.set(max);
			context.write(key, result);
			
	}
}


	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"WeatherDriver");
		job.setJarByClass(WeatherDriver.class);
		job.setMapperClass(WeatherMapper.class);
		job.setReducerClass(WeatherReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ?0 : 1);
	}


}
