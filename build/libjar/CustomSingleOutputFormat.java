package com.custom;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class CustomSingleOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	@Override
		protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
			return new Path(key.toString() + ".dat").toString();
		}

       @Override
       protected Text generateActualKey(Text key, Text value) {
             return null;
       }
 }
