package org.AutoTag.pkg;


import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.opencv.core.*;
import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;
import org.opencv.highgui.Highgui;

import com.google.common.primitives.UnsignedInteger;


  public class FeatureExtractMapper extends Mapper<HipiImageHeader, FloatImage, Text, Text> 
  {
	  //Load OpenCV library
	  static {System.loadLibrary(Core.NATIVE_LIBRARY_NAME);}
	  
	  public Mat extractFeatures(Mat m)
	  {
		  Mat grayImage = new Mat();
          m.convertTo(grayImage, CvType.CV_8U);
          
          // Create feature detector object with SIFT as operator that will extract key points
          FeatureDetector siftDetector = FeatureDetector.create(FeatureDetector.SIFT);
          MatOfKeyPoint keyPoint01 = new MatOfKeyPoint();
  		  siftDetector.detect(grayImage, keyPoint01);
          
  		  // For the key points extracted, Description extractor computes description 
  		  DescriptorExtractor siftExtractor = DescriptorExtractor.create(DescriptorExtractor.SIFT);
  		  Mat descriptors = new Mat(grayImage.rows(), grayImage.cols(), grayImage.type());
  		  siftExtractor.compute(grayImage, keyPoint01, descriptors); 
		  
  		 // Draw image 
  		 //System.out.println(descriptors.dump());
 		 //Mat outImage = new Mat(grayImage.rows(), grayImage.cols(), CvType.CV_8U);
 		 //Features2d.drawKeypoints(grayImage,keyPoint01, outImage);
 		 //Highgui.imwrite("/home/cloudera/Desktop/Feat.jpg", outImage);
 		 //System.out.println("Image copied");
  		  
  		 return descriptors;
		  
	  }
	  
	  public Mat convertToRows(Mat desc, int rtype)
	  {
		  Mat data = new Mat(desc.rows(), desc.cols(), rtype);
		  // Reshape converts the vector to matrix
		  if(desc.isContinuous())
			  desc.reshape(0, desc.rows()*desc.cols()).convertTo(data, rtype, 1, 0);
		  else 
			  desc.clone().reshape(0, desc.rows()*desc.cols()).convertTo(data, rtype, 1, 0);
		  
		  return data;
	  }
	  
      public void map(HipiImageHeader key, FloatImage value, Context context) 
      throws IOException, InterruptedException 
      {
    	if (value != null && value.getWidth() > 1 && value.getHeight() > 1 && value.getNumBands() == 3) {

            // Get dimensions of image
            int w = value.getWidth();
            int h = value.getHeight();

            // Get pointer to image data
            float[] valData = value.getData();

            // Initialize 3 element array to hold RGB pixel average
            float[] avgData = {0,0,0};
            
            // Creating Mat object
            Mat m = new Mat(h, w, CvType.CV_32F);
            
            // Traverse image pixel data in raster-scan order and convert into gray scale
            for (int j = 0; j < h; j++) {
              for (int i = 0; i < w; i++) {
                avgData[0] = valData[(j*w+i)*3+0] * 0.2989f; // R
                avgData[1] = valData[(j*w+i)*3+1] * 0.5870f; // G
                avgData[2] = valData[(j*w+i)*3+2] * 0.1140f; // B
                m.put(j,i,avgData);
              }
            }
            
            // Normalize the values of pixels
            Core.normalize(m, m, 0, 255, Core.NORM_MINMAX);
            
            // Extract features using SIFT
            Mat descriptors=extractFeatures(m);
         
            // Normalize using L2
            //Mat ma = new Mat(h, w, CvType.CV_32F);
            double alpha = 100;
            double beta = 100;
            int rows = descriptors.rows();
            int cols = descriptors.cols();
            Core.normalize(descriptors, descriptors, alpha, beta, Core.NORM_L2);
            //System.out.println(descriptors.dump());
            
            // Change the key to extract the image name
            String k=key + "";
            System.out.println("key: "+key);
            System.out.println(k);
            String temp[]=k.split("/");
            String t=temp[3].replace(".jpg\"}","");//3
         System.out.println("t: "+t);
          Configuration conf = context.getConfiguration();
      	  String param = conf.get("path");
      	  String qmode = conf.get("qmode");
            // Write the features into a file
            try {
         	     
            	String dump = descriptors.dump();
            	dump = dump.replaceAll(",", "\t");
            	dump = dump.replaceAll(";", "\n");
            	dump = dump.replaceAll("\\[","");
            	dump = dump.replaceAll("\\]","");
            	String file_mat = null;
            	//System.out.println("qmode in feature driver: " +qmode);
            	//qmode = "true";
               if(qmode.equals("true")){
            	  file_mat = "/home/cloudera/Desktop/project/qFeatures/"+t+".txt";
            		
            	}else{
            		file_mat = "/home/cloudera/Desktop/project/qFeatures/"+t+".txt";
            	}
            	// String file_mat = param+ "/" +t+".txt";
            	 PrintStream out = new PrintStream(new FileOutputStream(file_mat));
            	 System.setOut(out);         	     
         	     out.println(dump);
                out.close();
                String line = null;
                //String file_mat_tmp = "/home/cloudera/Desktop/Features/"+t+"_tmp"+".txt";
                
                FileReader fileReader = new FileReader(file_mat);
                BufferedReader bufferedReader = 
                new BufferedReader(fileReader);
                while((line = bufferedReader.readLine()) != null) {
                    System.out.println(line);
                    break;
                } 
                bufferedReader.close();
         	      
         	}
        	catch(Exception e)
         	{
         		System.out.println(e);
         	}
            context.write(new Text(t), new Text(descriptors.dump()) );
          } 
          
        }
    	
    }
