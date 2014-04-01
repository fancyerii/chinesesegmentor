package com.antbrains.crf.hadoop;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.antbrains.crf.FeatureDict;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

public class LoadFeatureDictAndTestMemory {

	public static void main(String[] args) throws Exception {
		if(args.length!=1){
			System.out.println("Usage LoadFeatureDictAndTestMemory dict");
			System.exit(-1);
		}

		FSTObjectInput foi=null;
		try{
			foi=new FSTObjectInput(new FileInputStream(args[0]));
			FeatureDict fd=(FeatureDict) foi.readObject();
			fd.size();
			double[] weights=new double[fd.size()*6];
			System.gc();
			long used = Runtime.getRuntime().totalMemory()
					- Runtime.getRuntime().freeMemory();
			System.out.println("HighFreq-Datrie memory usage: " + used+" bytes");
			System.out.println(weights);
			System.out.println(fd);
		}finally{
			if(foi!=null){
				foi.close();
			}
		}
		
	}

}
