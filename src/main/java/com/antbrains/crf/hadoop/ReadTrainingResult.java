package com.antbrains.crf.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.util.ReflectionUtils;

import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.TrainingParams;
import com.antbrains.crf.TrainingWeights;
import com.google.gson.Gson;

public class ReadTrainingResult {

	public static void main(String[] args) throws IOException {
		if(args.length<3){
			System.out.println("Usage ReadTrainingResult <hdfsIn> <confFile> <outModel> [conf1, conf2, ...]");
			System.exit(-1);
		}
		
		TrainingParams params=SgdCrf.loadParams(args[1]);
		System.out.println("Params: "+new Gson().toJson(params));
		Configuration conf = new Configuration();
		for (int i = 3; i < args.length; i++) {
			System.out.println("add resource: " + args[i]);
			conf.addResource(new Path(args[i]));
		}
 
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(args[0]);
		FileStatus[] status = fs.listStatus(inFile);
		
		for (FileStatus stat : status) {
			if (stat.isDir()) {
	
			} else {
				Path f = stat.getPath();
				if (f.getName().startsWith("_")) {
					System.out.println("ignore file: " + f.toString());
				} else {
					System.out.println(new java.util.Date()+" process: " + f.toString());
					
					SequenceFile.Reader reader = null;
					try{
						reader= new SequenceFile.Reader(fs, f, conf);
						Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
						
						if (reader.next(key, value)) {
							TrainingWeights weights=(TrainingWeights) value;
							System.out.println("save result");
							SgdCrf.saveModel(params, weights, args[2]);
							break;
						}
					}finally{
						if(reader!=null){
							reader.close();
						}
					}

				}
			}
		}
	}

}
