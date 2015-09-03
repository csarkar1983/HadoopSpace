package com.chanchal.pkg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class BundleSeq {
	
	/**
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws IOException,
            InstantiationException, IllegalAccessException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path("/infile");
        Path outputFile = new Path("/outfile/output");
        BufferedReader bufferReader;
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
										   	SequenceFile.Writer.file(outputFile),
											SequenceFile.Writer.keyClass(key.getClass()),
											SequenceFile.Writer.valueClass(value.getClass()));
        FileStatus[] fStatus = fs.listStatus(inputFile);

        for (FileStatus fst : fStatus) {
        	String line;
            String str = "";
            System.out.println("Processing file : " + fst.getPath().getName() + " and the size is : " + fst.getPath().getName().length());
            //bufferReader = new BufferedReader(new InputStreamReader(fs.open(inputFile)));
            //bufferReader = fs.open(fst.getPath());
            bufferReader = new BufferedReader(new InputStreamReader(fs.open(fst.getPath())));
            key.set(fst.getPath().getName());
            while ((line = bufferReader.readLine()) != null) {
                str = str+line+" ";
            }
            value.set(str);
            writer.append(key, value);

        }
        fs.close();
        IOUtils.closeStream(writer);
        System.out.println("SEQUENCE FILE CREATED SUCCESSFULLY........");
    }

}
