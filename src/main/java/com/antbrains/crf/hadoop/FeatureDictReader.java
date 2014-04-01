package com.antbrains.crf.hadoop;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.antbrains.crf.CompactedTroveFeatureDict;
import com.antbrains.crf.DATrieFeatureDict;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.TroveFeatureDict;

import de.ruedigermoeller.serialization.FSTObjectOutput;

public class FeatureDictReader {

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("Usage FeatureDictReader inDir outDict");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		for (int i = 2; i < args.length; i++) {
			System.out.println("add resource: " + args[i]);
			conf.addResource(new Path(args[i]));
		}
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		// conf.addResource(new
		// Path("/home/lili/soft/hadoop/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(args[0]);
		FileStatus[] status = fs.listStatus(inFile);
		//FeatureDict fd=new TroveFeatureDict(102400);
		FeatureDict fd=new CompactedTroveFeatureDict(102400);
		int lineNum=0;
		for (FileStatus stat : status) {
			if (stat.isDir()) {
				System.out.println("ignore subdir: "
						+ stat.getPath().toString());
			} else {
				Path f = stat.getPath();
				if (f.getName().startsWith("_")) {
					System.out.println("ignore file: " + f.toString());
				} else {
					System.out.println(new java.util.Date()+" process: " + f.toString());
					CompressionCodec codec = factory.getCodec(f);
					InputStream stream = null;

					// check if we have a compression codec we need to use
					if (codec != null) {
						stream = codec.createInputStream(fs.open(f));
					} else {
						stream = fs.open(f);
					}
					BufferedReader br = null;
					try {
						br = new BufferedReader(new InputStreamReader(
								stream, "UTF8"));
						String line;
						while ((line = br.readLine()) != null) {
							String[] arr=line.split("\t");
							fd.get(arr[0], true);
							lineNum++;
							
						}
					} finally {
						if (br != null) {
							br.close();
						}
					}
				}
			}
		}
		System.out.println(lineNum+"=?="+fd.size());
		FSTObjectOutput foo=null;
		try{
			foo=new FSTObjectOutput(new FileOutputStream(args[1]));
			foo.writeObject(fd);
		}finally{
			if(foo!=null){
				foo.close();
			}
		}
	}

}
