package com.antbrains.crf.hadoop;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.TrainingParams;
import com.antbrains.crf.TrainingWeights;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

public class ReverseFeatureDict {
	public static void main(String[] args) throws Exception{
		if(args.length!=2){
			System.out.println("Usage <in> <out>");
			System.exit(-1);
		}
		String featureFilename=args[0];
		FSTObjectInput foi=null;
		FeatureDict dict=null;
		System.out.println("load featuredict from: "+featureFilename);
		try{
			foi=new FSTObjectInput(new FileInputStream(featureFilename));
			dict=(FeatureDict) foi.readObject();
			
		}finally{
			if(foi!=null){
				foi.close();
			}
		}
		System.out.println(dict.size());
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1]),"UTF8"));
		 
		TObjectIntIterator<String> iter=dict.iterator();
		int line=0;
		while(iter.hasNext()){
			iter.advance();
			bw.write(iter.key()+"\t"+iter.value()+"\n");
			line++;
		}
		System.out.println(line);
		bw.close();
	}
}
