package com.antbrains;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.FileInputStream;
 	



import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class TestHdfs {

	public static void main(String[] args) throws Exception {
		long start=System.currentTimeMillis();
		Configuration conf = new Configuration();
		//conf.addResource(new Path("/home/lili/soft/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/home/lili/soft/hadoop/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Path f = new Path("/lili/instance_gen1/part-m-00000.deflate");
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(f);
		InputStream stream = null;

		// check if we have a compression codec we need to use
		if (codec != null) {
			stream = codec.createInputStream(fs.open(f));
		} else {
			stream = fs.open(f);
		}
		
		BufferedReader br=new BufferedReader(new InputStreamReader(stream,"UTF-8"));
		String line;
		long lineNum=0;
		while((line=br.readLine())!=null){
			System.out.println(line);
		}
		System.out.println("time used: "+(System.currentTimeMillis()-start)+" ms");
		System.out.println("total: "+lineNum);
		br.close();
	}

}
