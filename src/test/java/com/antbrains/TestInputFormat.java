package com.antbrains;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.FileInputStream;
 	



import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestInputFormat {

	public static void main(String[] args) throws Exception {
		long start=System.currentTimeMillis();
		Configuration conf = new Configuration();
		//conf.addResource(new Path("/home/lili/soft/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/home/lili/soft/hadoop-1.2.1/conf/core-site.xml"));
		conf.addResource(new Path("/home/lili/soft/hadoop-1.2.1/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path("/user/lili/output2");
		DataInput di;
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inFile),"UTF-8"));
		String line;
		long lineNum=0;
		while((line=br.readLine())!=null){
			lineNum++;
			if(lineNum%1000000==0){
				System.out.println(lineNum);
			}
		}
		System.out.println("time used: "+(System.currentTimeMillis()-start)+" ms");
		System.out.println("total: "+lineNum);
		br.close();
	}

}
