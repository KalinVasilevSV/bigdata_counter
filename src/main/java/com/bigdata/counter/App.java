package com.bigdata.counter;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * Hello world!
 *
 */
public class App {

    public static class TokenizerMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{
    	
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	
//    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//    		StringTokenizer itr = new StringTokenizer(value.toString());
//    		
//    		while(itr.hasMoreTokens()) {
//    			word.set(itr.nextToken());
//    			context.write(word, one);
//    		}
//    	}

		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
    			word.set(value.toString());
    			output.collect(word, one);
		}
    }
	
    public static class IntSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
    	
    	private IntWritable result = new IntWritable();
    	
//    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    		
//    		int sum = 0;
//    		for (IntWritable val: values) {
//    			sum += val.get();
//    		}
//    		result.set(sum);
//    		context.write(key, result);
//    	}

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
    		int sum = 0;
    		while(values.hasNext()) {
    			sum ++;
    			values.next();
    		}
    		output.collect(key, result);
			
		}
    }
    

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	JobConf jobconf = new JobConf(App.class);
    	jobconf.setJobName("word_count");
    	jobconf.setOutputKeyClass(Text.class);
    	jobconf.setOutputValueClass(IntWritable.class);
    	jobconf.setMapperClass(TokenizerMapper.class);
    	jobconf.setReducerClass(IntSumReducer.class);
    	jobconf.setInputFormat(TextInputFormat.class);
    	jobconf.setOutputFormat(TextOutputFormat.class);
    	
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(App.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(jobconf, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(jobconf,new Path("hdfs://localhost:9000/user/kalin/output"));
        
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        System.exit(JobClient.runJob(jobconf).isSuccessful() ? 0 : 1);
    }
    
}
