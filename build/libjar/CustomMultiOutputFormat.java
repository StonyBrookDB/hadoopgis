package com.custom;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class CustomMultiOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	@Override
		protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
			return new Path(key.toString(), leaf).toString();
		}

       @Override
       protected Text generateActualKey(Text key, Text value) {
             return null;
       }
 }
