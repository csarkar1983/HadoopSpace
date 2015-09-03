package com.chanchal.pkg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class TextToSequenceConverter 
{
	/**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {

        Configuration conf = new Configuration();
        //conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        //conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path("/infile/wordcountproblem");
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(inputFile)));
        Path outputFile = new Path("/outfile/output");
        IntWritable key = new IntWritable();
        int count = 0;
        Text value = new Text();    
        String str;
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
        													   	SequenceFile.Writer.file(outputFile),
        														SequenceFile.Writer.keyClass(key.getClass()),
        														SequenceFile.Writer.valueClass(value.getClass()));
        while ((str = bufferReader.readLine()) != null) {
            key.set(count++);
            value.set(str);
            writer.append(key, value);
        }
        fs.close();
        IOUtils.closeStream(writer);
        System.out.println("SEQUENCE FILE CREATED SUCCESSFULLY........");
    }
}
