package com.antbrains;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.TrainingParams;
import com.antbrains.crf.TrainingWeights;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

public class Test {
	public static void main(String[] args) throws Exception{
		String featureFilename="/home/lili/featuredict6";
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
		TIntObjectHashMap<String> map=new TIntObjectHashMap<String>();
		TObjectIntIterator<String> iter=dict.iterator();
		while(iter.hasNext()){
			iter.advance();
			System.out.println(iter.key()+"\t"+iter.value());
			map.put(iter.value(), iter.key());
		}
		FSTObjectOutput foo=null;
		try{
			foo=new FSTObjectOutput(new FileOutputStream("/home/lili/revfeaturedict6"));
			foo.writeObject(map);
		}finally{
			if(foo!=null){
				foo.close();
			}
		}
	}
}
