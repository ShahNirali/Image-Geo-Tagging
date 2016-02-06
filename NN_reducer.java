package org.AutoTag.pkg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

  public class NN_reducer extends Reducer<Text, Text, Text, Text> 
  {
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	String param = conf.get("k");
    	int k = Integer.parseInt(param);
    	List<Double> distances = new ArrayList<Double>();
    	List<String> names = new ArrayList<String>();
    	for(Text value : values){
    		String[] parts = value.toString().split("|");
    		names.add(parts[0]);
    		distances.add(Double.parseDouble(parts[1]));   
    		
    	}
    	double min_distance = 10000;
    	int pos = 0;
    	for(int i = 0; i < k; i++){
    		for(int j = i; i < distances.size(); i++){
    			if(distances.get(j) < min_distance){
    				min_distance =distances.get(j);
    				pos = j;
    			}
    			
    		}
    		double temp = distances.get(i);
    		distances.set(i, distances.get(pos));
    		distances.set(pos, temp);
    		
    		String tmp = names.get(i);
    		names.set(i, names.get(pos));
    		names.set(pos, tmp);
    	}
    	for(int i = 0; i < k ; i++){
    		context.write(key, new Text(names.get(i)));
    	}
        
        }
      } 
 

