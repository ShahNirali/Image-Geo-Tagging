package org.AutoTag.pkg;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

  public class NN_mapper extends Mapper<Text, Text, Text, Text> 
  {
	// public static String query;
//	  public void configure(JobConf job) {
//		    query = job.get("query").toString();
//	  }	  
      public void map(Text key, Text value, Context context) 
      throws IOException, InterruptedException 
      {
    	
    	  Configuration conf = context.getConfiguration();
    	  String param = conf.get("query");
    	  
    	  //param = param.replace("{", "");
    	  //param = param.replace("}", "");
    	  FileSystem fs = FileSystem.get(conf);
    	  SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(param),conf);
    	  Text qkey = new Text();
    	  Text qvalue = new Text();
    	  int i = 0;
    	  while(reader.next(qkey,qvalue)){
    		  i = i + 1;
    	  
    		  String qname = qkey.toString();
    		  String qbof_val = qvalue.toString();
    		  System.out.println(qbof_val);
    		//  Vector bof = new DenseVector();
        	  //String qbof_val = value.toString();
        	  qbof_val = qbof_val.replace("{","");
        	  qbof_val = qbof_val.replace("}","");
        	  System.out.println(qbof_val);
        	  String[] qb_splits = qbof_val.split(",");
        	  Vector qb_vec = new DenseVector(qb_splits.length);
        	  for(String qb_split:qb_splits){
        		  System.out.println(qb_split);
        		  String[] qb_val = qb_split.split(":");
        		  System.out.println("b_val: " +qb_val[0]);
        		  System.out.println("b_val: "+qb_val[1]);
        		  qb_vec.set(Integer.parseInt(qb_val[0]), Double.parseDouble(qb_val[1]));
        	  }
        	  
    		  
    	  
    	  
    	  
//    	  String[] splits = param.split(",");
//    	  Vector q_vec = new DenseVector(splits.length);
//    	  System.out.println(q_vec);
//    	  for(String split:splits){
//    		  String[] w_val = split.split(":");
//    		  q_vec.set(Integer.parseInt(w_val[0]), Double.parseDouble(w_val[1]));
//    	  }
    	 // Vector bof = new DenseVector();
    	  String bof_val = value.toString();
    	  bof_val = bof_val.replace("{","");
    	  bof_val = bof_val.replace("}","");
    	  System.out.println(bof_val);
    	  String[] b_splits = bof_val.split(",");
    	  Vector b_vec = new DenseVector(b_splits.length);
    	  for(String b_split:b_splits){
    		  String[] b_val = b_split.split(":");
    		  System.out.println("b_val: " +b_val[0]);
    		  System.out.println("b_val: "+b_val[1]);
    		  b_vec.set(Integer.parseInt(b_val[0]), Double.parseDouble(b_val[1]));
    	  }
    	  DistanceMeasure measure = new EuclideanDistanceMeasure();
    	  double distance = measure.distance(
                  b_vec.getLengthSquared(), 
                  b_vec,qb_vec);
 
    	    System.out.println(distance);
    	  	System.out.println(key);
            System.out.println(value);
            System.out.println(param);
            
           // int q_num = 1;
            String val_to_write = key.toString() + "|" + Double.toString(distance);
            
            // Emit record to reducer
            context.write(new Text(qname), new Text(val_to_write));
    	  }

          } // If (value != null...
          
        } // map()
    	
    