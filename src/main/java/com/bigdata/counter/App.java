package com.bigdata.counter;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
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

		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String[] tokens = value.toString().split(DELIM);
			
			if(index == null) {
				index = findIndex(tokens,PAYMENT_TYPE);
				return;
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
				return;
			}
			
			word.set(tokens[index]);
			output.collect(word, one);
		}
    }
    
    public static class DateMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{

    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private Integer index;

    	SimpleDateFormat pointFormat = new SimpleDateFormat("MM.dd.yy hh:mm");
    	SimpleDateFormat slashFormat = new SimpleDateFormat("MM/dd/yy hh:mm");
    	SimpleDateFormat slashFormatter = new SimpleDateFormat("MM/dd/yy");
    	
		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String[] tokens = value.toString().split(DELIM);
			
			if(index == null) {
				index = findIndex(tokens,TRANSACTION_DATE);
				return;
			}

			Date date = null;
			try {
				date = slashFormat.parse(tokens[index]);
			} catch (ParseException e) {
				try {
				date = pointFormat.parse(tokens[index]);
				} catch (ParseException ex) {
					ex.printStackTrace();
				}
			}
			
//			LocalDate writableDate = LocalDate.of(date.getYear(),date.getMonth(),date.getDate());
//			
//			System.out.println(writableDate.toString());
			
			word.set(slashFormatter.format(date));
			output.collect(word, one);
		}
    	
    }
	
    public static class IntSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
    	
    	private IntWritable result = new IntWritable();
    	
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
        
        FileInputFormat.setInputPaths(paymentJob, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(paymentJob,new Path("hdfs://localhost:9000/user/kalin/output/payment"));
        
        FileInputFormat.setInputPaths(productJob, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(productJob,new Path("hdfs://localhost:9000/user/kalin/output/product"));
        
        FileInputFormat.setInputPaths(dateJob, new Path("hdfs://localhost:9000/user/kalin/input/sales.csv"));
        FileOutputFormat.setOutputPath(dateJob,new Path("hdfs://localhost:9000/user/kalin/output/date"));

        JobControl control = new JobControl("job_control");
        control.addJob(new Job(paymentJob));
        control.addJob(new Job(productJob));
        control.addJob(new Job(dateJob));
        
        control.run();
    }
    
    private static Integer findIndex(String[] tokens, String key) {
		for(int i=0;i<tokens.length;i++) {
			if(key.equals(tokens[i]))
				return i;
		}
		return null;
    }
}
