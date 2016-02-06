package org.AutoTag.pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hipi.imagebundle.mapreduce.HibInputFormat;

public class FeatureExtractorDriver extends Configured{// implements Tool {
  
  public static int run(String input, String output,boolean q_mode) throws Exception {

	Job job = Job.getInstance();
	Configuration conf = new Configuration();
    conf.set("path", output );
    System.out.println("driver :" + q_mode);
    if(q_mode){
    	conf.set("qmode", "true");
    }else{
    	conf.set("qmode", "false");
    	
    }
    job.setInputFormatClass(HibInputFormat.class);
    job.setJarByClass(FeatureExtractorDriver.class);
    job.setMapperClass(FeatureExtractMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    boolean success = job.waitForCompletion(true);      
    return success ? 0 : 1;  
  }

  
}