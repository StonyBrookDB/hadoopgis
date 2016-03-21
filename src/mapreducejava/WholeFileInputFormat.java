package com.custom;

import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.custom.WholeFileRecordReader;

public class WholeFileInputFormat extends TextInputFormat {
	@Override	
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
	

/*
	@Override
		public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {
        		return null;
			//return new WholeFileRecordReader((FileSplit) split, job);
    		}
*/
}


