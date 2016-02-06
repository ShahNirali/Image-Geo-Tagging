package org.AutoTag.pkg;


import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
//import java.io.Writer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
//import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.clustering.classify.ClusterClassifier;
import org.apache.mahout.clustering.iterator.ClusterIterator;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.iterator.KMeansClusteringPolicy;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.display.DisplayClustering;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;
public class Kmeans extends DisplayClustering {

	public static void run1(String input,String output,boolean qmode) throws Exception{
		System.out.println("kmeans---------------------start--------------");
		Configuration conf = new Configuration();
		System.out.println(qmode);
		input = "/home/cloudera/Desktop/project/Features";
		if(!qmode){
			System.out.println("---kmeans qmode not working-----");
			Path tmp_output = new Path("/home/cloudera/Desktop/project/Kmeansoutput");
			System.out.println(input);
			
		
			HadoopUtil.delete(conf,tmp_output);
			boolean runClusterer = true;
			if (runClusterer) {
				run_t(conf,new Path(input), tmp_output, new
						EuclideanDistanceMeasure(), 3, 0.5, 10);
			} else {
				runSequentialKMeansClassifier(conf, new Path(input), tmp_output, new
						EuclideanDistanceMeasure(), 3, 10, 0.5);
			}
		}
		System.out.println("bof:"+output);
		
		List<Vector> centroidlist = new ArrayList<Vector>();
		FileSystem fs = FileSystem.get(conf);
		String file_path = "/home/cloudera/Desktop/project/Kmeansoutput/";//"/home/cloudera/Desktop/project/Kmeansoutput/";
		File folder = new File(file_path);//tmp_output.toString());
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
			} else if (listOfFiles[i].isDirectory()) {
				if(listOfFiles[i].getName().startsWith("cluster") && listOfFiles[i].getName().contains("final")){
					File folder1 = new File(file_path + listOfFiles[i].getName());
					File[] listOfFiles1 = folder1.listFiles();
					for (int j = 0; j < listOfFiles1.length; j++) {
						if (listOfFiles1[j].isFile()) {
							if(listOfFiles1[j].getName().startsWith("part")){
								SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(file_path+listOfFiles[i].getName()+"/"+listOfFiles1[j].getName()),conf);
								IntWritable key = new IntWritable();
								ClusterWritable value = new ClusterWritable();
								while(reader.next(key, value)){
									System.out.println(key);
									System.out.println(value.getValue());
									String val = value.getValue().toString();
									// Pattern p = new Pattern();
									Pattern p = Pattern.compile("(.*?)c=(.*?) r=(.*?)",Pattern.DOTALL);
									Matcher m = p.matcher(val);
									if(m.matches() && (m.groupCount() == 3)){
										String centroid = m.group(2);
										centroid = centroid.replaceAll("\\[","");
						            	centroid = centroid.replaceAll("\\]","");
										String[] data = centroid.toString().split(",");
										Vector vec = new RandomAccessSparseVector(data.length);
										for (int k = 0; k < data.length; k++) {
											if (!data[k].equals("0")) {
												vec.set(k, Double.parseDouble(data[k]));
											}
										}
										//System.out.println("add");
										centroidlist.add(vec);													  
									}

								}
								reader.close();

							}
						} 

					}
				}
			}
		}
		Path bof = null;
		if(qmode){
			 bof = new Path("/home/cloudera/Desktop/project/qbof");
			
		}else{
			
			bof = new Path("/home/cloudera/Desktop/project/bof");
		}
		//Path bof = new Path(output);
		 Text key_seq = new Text();
		 Text value_seq = new Text();
		 SequenceFile.Writer writer = null; 
		// PrintWriter writer = new PrintWriter("/home/cloudera/Desktop/project/qbof", "UTF-8");
    		 
		 Configuration confHadoop = new Configuration();     
	        confHadoop.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
	        confHadoop.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));   
	        FileSystem fs1 = FileSystem.get(confHadoop);
	        writer = SequenceFile.createWriter(fs1, confHadoop, bof, key_seq.getClass(),value_seq.getClass());
	        
		  String src_path = null;
		  if (qmode){
			  src_path = "/home/cloudera/Desktop/project/qFeatures/";
		  }else{
			  src_path = "/home/cloudera/Desktop/project/Features/";
			  
		  }
		  EuclideanDistanceMeasure measure = new EuclideanDistanceMeasure();
		  File src_f = new File(src_path);//input);
		  File[] listOf_f = src_f.listFiles();
		  String line = null;
		  List<Vector> count_veclist = new ArrayList<Vector>();
		  for(File f: listOf_f){
			  FileReader fileReader = new FileReader(src_path + f.getName());
		            BufferedReader bufferedReader = new BufferedReader(fileReader);
		            List<Vector> veclist1 = new ArrayList<Vector>();
		            Vector count_vec = new DenseVector(centroidlist.size());//RandomAccessSparseVector(centroidlist.size());
		            while((line = bufferedReader.readLine()) != null) {
		            	if(line.isEmpty()){
		            		continue;
		            	}
		            	String[] fvecs = line.split("\t");
		                Vector vec1 = new DenseVector(fvecs.length);//DenseVector(line.length());
		            	int l = 0;
		            	for(String fvec: fvecs){
		            		if(!fvec.isEmpty()){
		            			if(!fvec.contains("crc")){
		            			//System.out.println("fvec: "+fvec);
		            			vec1.set(l, Double.parseDouble(fvec));
		            			l++;
		            			}
		            			
		            		}
		            	}
		            	//System.out.println(vec1);
		            	//System.out.println(vec1.toString());
		            	if(!(vec1.toString().equals("{}"))){
			            	double nearestDistance = 10000;
			            	int nearestCluster = 0;
			            	for (int i = 0; i < centroidlist.size(); i++) {
		                        double distance = measure.distance(
		                                        centroidlist.get(i).getLengthSquared(), 
		                                        centroidlist.get(i), vec1);
		                        if ((distance < nearestDistance)) {
		                                nearestCluster = i;
		                                nearestDistance = distance;
		                        }     
			                }
			                double val3 = count_vec.get(nearestCluster);
			                val3 += 1;
			                count_vec.set(nearestCluster, val3);
			            	veclist1.add(vec1);
		            	}
		            }
		            System.out.println("bof: " + count_vec);
		            String f_out = f.getName() + "|" + count_vec;
		            //.println(f_out);
		            
		           
		          //  writer.close();
		            writer.append(new Text(f.getName()), new Text(count_vec.toString()));
		  }
		  writer.close();
		  

	}
	public static void run_t(Configuration conf, Path input, Path
			output1, DistanceMeasure measure, int k,
			double convergenceDelta, int maxIterations) throws Exception {
		// Input should be given as sequence file format
		String dconv = "/home/cloudera/Desktop/project/Kmeansdataconverted";
		Path directoryContainingConvertedInput = new Path(output1,dconv);//DIRECTORY_CONTAINING_CONVERTED_INPUT);
		InputDriver.runJob(input, directoryContainingConvertedInput,
				"org.apache.mahout.math.RandomAccessSparseVector");
		// Get initial clusters randomly

		Path clusters = new Path(output1, "random-seeds");
		clusters = RandomSeedGenerator.buildRandom(conf,
				directoryContainingConvertedInput, clusters, k, measure);
		// Run K-Means with a given K
		KMeansDriver.run(conf, directoryContainingConvertedInput,
				clusters, output1, convergenceDelta,
				maxIterations, true, 0.0, true);
		loadClustersWritable(output1);

	}

	private static  void runSequentialKMeansClassifier(Configuration conf, Path samples, Path output1,
			DistanceMeasure measure, int numClusters, int maxIterations, double convergenceDelta) throws IOException {
		Collection<Vector> points = Lists.newArrayList();
		for (int i = 0; i < numClusters; i++) {
			points.add(SAMPLE_DATA.get(i).get());
		}
		List<Cluster> initialClusters = Lists.newArrayList();
		int id = 0;
		for (Vector point : points) {
			initialClusters.add(new org.apache.mahout.clustering.kmeans.Kluster(point, id++, measure));
		}
		ClusterClassifier prior = new ClusterClassifier(initialClusters, new KMeansClusteringPolicy(convergenceDelta));
		Path priorPath = new Path(output1, Cluster.INITIAL_CLUSTERS_DIR);
		prior.writeToSeqFiles(priorPath);

		ClusterIterator.iterateSeq(conf, samples, priorPath, output1, maxIterations);
		loadClustersWritable(output1);
	}
}

