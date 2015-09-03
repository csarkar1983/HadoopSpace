package com.chanchal.pkg;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountusingSequenceFile {
	

	public static class WordCountMap extends Mapper<Text, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);

		public void map(Text key, Text value, Context context)
               throws IOException, InterruptedException {
		   String line=value.toString();
		   line.replaceAll("[\t\n\r]", " ");
           String[] values = line.split(" ");
           for(String word: values) {
               context.write(new Text(word), ONE);
           }
		}
	}

	public static class SumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values){
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		//GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        //Configuration conf = parser.getConfiguration();
        //String[] args = parser.getRemainingArgs();
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("wordcount");

        job.setJarByClass(WordCountusingSequenceFile.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(SumReduce.class);
   
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
	}

}
