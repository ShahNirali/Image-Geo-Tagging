package org.AutoTag.pkg;

import java.util.*;
import java.io.*;

import org.AutoTag.pkg.FeatureExtractorDriver;
import org.AutoTag.pkg.Kmeans;
import org.AutoTag.pkg.NN_driver;
import org.apache.hadoop.fs.Path;
public class Autotag_main_class {
	
	public static void main(String[] args) {
		   if (args.length != 4) {
			      System.out.println("Usage: Autotag <input directory> <query directory>/\"No Query\" <output directory>");
			      System.exit(0);
		   } 
		   int res = 1;
		   
		   try{
			   boolean q_mode = true;
			   System.out.println(args[1]);
			   if(args[3].equals("1")){
				 //  System.out.println("true");
				   q_mode = false;
			   }
			   //System.exit(1);
			   String HibImport_path = "/home/cloudera/hipi/tools/hibImport/build/libs/hibImport.jar";
			   Runtime rt = Runtime.getRuntime();
			   Path tmp = new Path(args[0]);
			   String tmp1 = tmp.getParent().toString();
			   String tmp_name = tmp.getName();
			   String tmp_hib_path = tmp1 +"/" + tmp_name + ".hib";
			   String tmp_f_path = "/home/cloudera/Desktop/project/Features/";
			   
			   Path temp =  new Path(args[1]);
			   
			  if(!q_mode){
				   System.out.println("-----query mode-----");
				   String tmp2 = temp.getParent().toString();
				   String temp_name = temp.getName();
				   String q_hib_path = "/home/cloudera/Desktop/project/query.hib";//tmp2 + "/" + "query" + ".hib";
				   String q_f_path = "/home/cloudera/Desktop/project/qout/";
				   System.out.println(args[1]);
				   String q_path = "/home/cloudera/Desktop/project/query";
				   Process prq = rt.exec("java  -jar "+HibImport_path+" "+ q_path + " " + q_hib_path);
				   prq.waitFor();
				   res = FeatureExtractorDriver.run(q_hib_path,q_f_path,q_mode);
				   res = 0;
				   if(res == 0){
					   String tmp_b_path = tmp2+"/"+"qbof";
					   System.out.println(tmp_b_path);
					   File f = new File(tmp_b_path);
//					   if(!(f.exists() && f.isFile())) { 
//						   PrintWriter writer = new PrintWriter("tmp_b_path", "UTF-8");
//						   writer.close();
//					   }
					   q_f_path = "/home/cloudera/Desktop/project/qFeatures";
					   Kmeans.run1(q_f_path,tmp_b_path,q_mode);
					  

					   
					   res = NN_driver.run(tmp_b_path); 
				  
			   }
			}else{
			   //System.out.println(tmp_hib_path);
			   //System.exit(1);
			   Process pr = rt.exec("java  -jar "+HibImport_path+" "+ args[0] + " " + tmp_hib_path);
			   System.out.println("----------hib file prep done----------------------------------");
			   
			  System.out.println(pr.getInputStream());
			  pr.waitFor();  
			  System.out.println("------------------Start feature extraction---------------------");
			  res = FeatureExtractorDriver.run(tmp_hib_path,"out",q_mode);
			   res = 0;
			   if(res == 0){
				   String tmp_b_path = tmp1+"/"+"bof3";
				   System.out.println(tmp_b_path);
				   File f = new File(tmp_b_path);
				   if(!(f.exists() && f.isFile())) { 
					   PrintWriter writer = new PrintWriter("tmp_b_path", "UTF-8");
					   writer.close();
				   }
				   System.out.println("------------------Kmeans and Bof prep start---------------------");
				   System.out.println(tmp_f_path);
				   System.out.println(tmp_b_path);
				   Kmeans.run1(tmp_f_path,tmp_b_path,q_mode);
				//   res = NN_driver.run();  				   
			  }
			}
		   }catch(Exception e){
			   e.printStackTrace();
			   System.out.println(e.getMessage());
			   System.exit(1);
		   }
		   
		   
	}
	
	

}
