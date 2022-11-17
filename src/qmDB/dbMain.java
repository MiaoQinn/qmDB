package qmDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class dbMain {
 
 // main function  
 public static void main(String [] args) throws IOException
 {
  while(true)
  { 
   System.out.println("Please input the query you wish to conduct: ");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in)); 
    String sql = reader.readLine(); 
    if(sql.compareTo(".exit")==0)
    { 
     System.out.println("Exit the program");
     break; 
    }
    else
    {
     System.out.println("Incorrect sql sentence of"+"'"+sql+"'");
    }
    
  }
 }
}