//Display files from Hadoop FS on stdout

//Run with the command hadoop jar <FileCopyWithProgress.jar> <local_file> hdfs://sandbox:8020/user/root/sample.txt
//Run with local file (local_file) and hdfs file for output

package com.altisource.hadoop.filesystem;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

 

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;



public class FileCopyWithProgress {
	
	 
	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		
		String localSrc = args[0];
		String dst = args[1];
		
	
		InputStream in = new BufferedInputStream (new FileInputStream(localSrc));
		
		try {
			Configuration conf = new Configuration ();
			FileSystem fs = FileSystem.get (URI.create(dst), conf);
			
			OutputStream out = fs.create (new Path(dst), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});
			
			IOUtils.copyBytes(in,out,4096,true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}

	}

}
