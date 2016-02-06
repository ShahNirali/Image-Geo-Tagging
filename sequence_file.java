package com.test_project.pkg;

import java.io.*;
import java.util.*;
import java.net.*;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Writable;

public class sequence_file {
	    public static void main(String args[]) throws Exception {

	        Configuration confHadoop = new Configuration();     
	        confHadoop.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
	        confHadoop.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));   
	        FileSystem fs = FileSystem.get(confHadoop);
	        Path outPath = new Path("project/seqfile");
	        //FileSystem fs = FileSystem.get(URI.create(uri), confHadoop);
	        FSDataInputStream in = null;
	        Text key = new Text();
	        BytesWritable value = new BytesWritable();
	        SequenceFile.Writer writer = null;
	        int i = 0;
	       // int j = 0;
	        Path sourcepath = new Path("project/dataset/");
	        FileStatus[] statuses = fs.listStatus(sourcepath);
	        for(FileStatus status: statuses){
	        	Path child = status.getPath();
	        	FileStatus[] childstatuses = fs.listStatus(child);
	        	i = i + 1;
	        	if(i > 10){
	        		break;
	        	}
	        	 int j = 0;
	        	for(FileStatus childst: childstatuses){
	        		System.out.println(childst.getPath());
			        Path inPath = childst.getPath();
			        if(!fs.exists(inPath)){
			        	System.out.println("No input file exists.");
			        	System.exit(-1);
			        }else{
			        	//System.out.println("file exists");
			        }
	        
			        try{
			            in = fs.open(inPath);
			            byte[] buffer = null;//[] = new byte[in.available()];
			            //byte[] buffer = IOUtils.toByteArray(in);
			            //byte[] buffer = null;
			            //int bytesRead;
			            //while ((bytesRead = in.read(buffer)) > 0) {
			            	//  out.write(buffer, 0, bytesRead);
			            	//}
			            
			            in.read(buffer);
			           // System.out.println(buffer);
			            
			            writer = SequenceFile.createWriter(fs, confHadoop, outPath, key.getClass(),value.getClass());
			            writer.append(new Text(inPath.getName()), new BytesWritable(buffer));
			        }catch (Exception e) {
			            System.out.println("Exception MESSAGES = "+e.getMessage());
			        }
		        }
	    }
	        //finally {
	            IOUtils.closeStream(writer);
	            System.out.println("over");
	        //}*/
	    
	    }
}
//}