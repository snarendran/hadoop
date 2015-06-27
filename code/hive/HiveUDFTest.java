package com.altisource.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class HiveUDFTest extends UDF {
		 
	//Returns upper case
	public Text evaluate(Text name) {
		if (name == null) 
			return null ;
		else 
			return new Text (name.toString().toUpperCase());
	}
	
	
	/** 
	 * @param age
	 * @return twice the age
	 * 
	 * The data type has to match the Hive column date type. For example, in Hive BigInt = Java long
	 * If the data type does not match, then no error is returned (thats bad!!). Hive simply does not call this function
	 * Second, because of SOR, if the cell value has a type error, say N/A or null for the age field, then a runtime (not compile time) exception 
	 * is returned by Hive.
	 */
	public long evaluate(long  age) {
		
		try {
			return   (long)2*age;
		} catch (Exception e) {
			
		}
		
		return -1;
	}
	
	public static void main(String[] args) {
		
	}
	 

}
