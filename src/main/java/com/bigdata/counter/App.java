package com.bigdata.counter;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.text.matcher.StringMatcher;
import org.apache.commons.text.matcher.StringMatcherFactory;
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
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

/**
 * Hello world!
 *
 */
public class App {
	
	private static final String DELIM = ",";
	private static final String PAYMENT_TYPE = "Payment_Type";
	private static final String PRODUCT = "Product";
	private static final String TRANSACTION_DATE = "Transaction_date";

    public static class PaymentMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{
    	
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private Integer index;
    	
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
			
			String[] tokens = value.toString().split(DELIM);
			
			if(index == null) {
				index = findIndex(tokens,PAYMENT_TYPE);
			}
			
			word.set(tokens[index]);
			output.collect(word, one);
		}
    }
    
    public static class ProductMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{

    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private Integer index;
    	
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String[] tokens = value.toString().split(DELIM);
			
			if(index == null) {
				index = findIndex(tokens,PRODUCT);
			}
			
			word.set(tokens[index]);
			output.collect(word, one);
		}
    }
    
    public static class DateMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{

    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private Integer index;
    	
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String[] tokens = value.toString().split(DELIM);
			
			if(index == null) {
				index = findIndex(tokens,TRANSACTION_DATE);
			}
			
			word.set(tokens[index]);
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
    		result.set(sum);
    		output.collect(key, result);
		}
    }
    

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	JobConf paymentJob = new JobConf(App.class);
    	paymentJob.setJobName("payment_job");
    	paymentJob.setOutputKeyClass(Text.class);
    	paymentJob.setOutputValueClass(IntWritable.class);
    	paymentJob.setMapperClass(PaymentMapper.class);
    	paymentJob.setReducerClass(IntSumReducer.class);
    	paymentJob.setInputFormat(TextInputFormat.class);
    	paymentJob.setOutputFormat(TextOutputFormat.class);
    	
    	JobConf productJob = new JobConf(App.class);
    	productJob.setJobName("product_job");
    	productJob.setOutputKeyClass(Text.class);
    	productJob.setOutputValueClass(IntWritable.class);
    	productJob.setMapperClass(ProductMapper.class);
    	productJob.setReducerClass(IntSumReducer.class);
    	productJob.setInputFormat(TextInputFormat.class);
    	productJob.setOutputFormat(TextOutputFormat.class);
    	
    	
    	JobConf dateJob = new JobConf(App.class);
    	dateJob.setJobName("date_job");
    	dateJob.setOutputKeyClass(Text.class);
    	dateJob.setOutputValueClass(IntWritable.class);
    	dateJob.setMapperClass(DateMapper.class);
    	dateJob.setReducerClass(IntSumReducer.class);
    	dateJob.setInputFormat(TextInputFormat.class);
    	dateJob.setOutputFormat(TextOutputFormat.class);
    	
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(App.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(paymentJob, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(paymentJob,new Path("hdfs://localhost:9000/user/kalin/output/payment"));
        
        FileInputFormat.setInputPaths(productJob, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(productJob,new Path("hdfs://localhost:9000/user/kalin/output/product"));
        
        FileInputFormat.setInputPaths(dateJob, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(dateJob,new Path("hdfs://localhost:9000/user/kalin/output/date"));
        
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        JobControl control = new JobControl("job_control");
        control.addJob(new Job(paymentJob));
        control.addJob(new Job(productJob));
        control.addJob(new Job(dateJob));
        
        control.run();
        
//        System.exit(JobClient.runJob(paymentJob).isSuccessful() ? 0 : 1);
    }
    
    private static Integer findIndex(String[] tokens, String key) {
		for(int i=0;i<tokens.length;i++) {
			if(key.equals(tokens[i]))
				return i;
		}
		
		return null;
    }
}
