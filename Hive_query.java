package org.AutoTag.pkg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
//import org.apache.hadoop.hive.jdbc.HiveDriver;



public class Hive_query{
  //private static String driver = "org.apache.hadoop.hive.jdbc.HiveDriver";

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
	  try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Connection connect = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
    Statement stmt = connect.createStatement();
    stmt.execute("drop table dataset");
    stmt.execute("create external table dataset (img string, loc string, long string, lat string, tag string) row format delimited fields terminated by '|' location '/user/cloudera/hive_project/'");
    String part_q = "select * from dataset where img =";
   // ResultSet res = stmt.executeQuery("select * from dataset where img = '9c7f5d048fcd84e3309ba11968eb6953.jpg '" );
  //  ResultSet res = stmt.executeQuery("select * from dataset");
//       while(res.next()){
//      //System.out.println(res.getRow());
//      	System.out.println(res.getString("img"));
////      	System.out.println(res.getString("loc"));
//      	System.out.println(res.getString("long"));
//      	System.out.println(res.getString("lat"));
//      }
  try{
    FileReader fileReader1 = new FileReader("/home/cloudera/Desktop/project/matches/match2");
    BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
    String line1 = null;
    while((line1 = bufferedReader1.readLine()) != null) {
       	if(!line1.isEmpty()){
       		String[] parts = line1.split("\t");
       		String query_n = parts[0];
       		query_n =  query_n.replace(".txt", ".jpg");
       		String ans = parts[1];
       		ans = ans.replace(".txt", ".jpg");
       	//	System.out.println(ans);
       		
       		String full_q = part_q +"'" +ans+" '";
       	//	System.out.println(full_q);
       		ResultSet res = stmt.executeQuery(full_q);
       		while(res.next()){
       			System.out.println("query_result");
       			System.out.println(res.getString("long"));
       			System.out.println(res.getString("lat"));
       		}
       		res.close();
       		String sec_q = part_q + "'"+query_n+" '";
       		res = stmt.executeQuery(sec_q);
       		while(res.next()){
       			System.out.println("actual_result");
       			System.out.println(res.getString("long"));
       			System.out.println(res.getString("lat"));
       			
       		}
       		res.close();
       		
       	}
       		
       	}
  }catch(Exception e){
	  e.printStackTrace();
  }

    
//    
//    ResultSet res = stmt.executeQuery("select * from dataset where img = 'f797a0938b26ec6f1a8f5102edddb381.jpg '");
//    //System.out.println(res.getString(1));
//    while(res.next()){
//    //System.out.println(res.getRow());
//    	System.out.println(res.getString("img"));
////    	System.out.println(res.getString("loc"));
////    	System.out.println(res.getString("lon"));
////    	System.out.println(res.getString("lat"));
//    }
    
    // execute statement
    //stmt.executeQuery("SHOW TABLES");
       
//    System.out.println("Table employee created.");
    connect.close();

  }
}
