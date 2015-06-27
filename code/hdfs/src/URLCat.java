//Display files from Hadoop FS on stdout
//Run with the command hadoop jar <URLCat.jar> hdfs://sandbox:8020/user/root/sample.txt
//Do not run with localhost as the sandbox cannot connect to localhost

package com.altisource.hadoop.filesystem;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;


public class URLCat {
	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		
		InputStream in = null;
		try {
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in,System.out,4096,false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}

	}

}
