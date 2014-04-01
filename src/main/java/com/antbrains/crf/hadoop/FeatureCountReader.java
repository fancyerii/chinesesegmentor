package com.antbrains.crf.hadoop;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
 






import java.util.ArrayList;

import com.antbrains.crf.BESB1B2MTagConvertor;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.TagConvertor;
import com.antbrains.crf.TrainingWeights;
import com.google.gson.Gson;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

public class FeatureCountReader {

	public static void main(String[] args) throws Exception {
		if(args.length!=1){
			System.out.println("Usage: FeatureCountReader <featuredict>");
			System.exit(-1);
		}
		FSTObjectInput foi = null;
		FeatureDict fd=null;
		System.out.println(new java.util.Date()+" start load");
		try {
			foi = new FSTObjectInput(new FileInputStream(args[0]));
			try {
				fd = (FeatureDict) foi.readObject();
				System.out.println(fd.size());
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			
		} finally {
			if (foi != null) {
				foi.close();
			}
		}
		 
	}
	
	public static void addArray(ArrayList<String> lines,double[] array){
		lines.add(""+array.length);
		for(double d:array){
			lines.add(""+d);
		}
	}

}
