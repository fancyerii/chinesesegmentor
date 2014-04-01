package com.antbrains;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import com.antbrains.crf.CompactedTroveFeatureDict;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.TroveFeatureDict;
import com.antbrains.crf.hadoop.FileTools;

 
public class TestFeatureDict {

	public static void main(String[] args) throws IOException {
		 
		String dict = "/usr/share/dict/words";
		Random rnd=new Random(0);
		FeatureDict fd=new CompactedTroveFeatureDict(10240);
		List<String> lines=FileTools.readFile2List(dict, "UTF8");
		TObjectIntHashMap<String> map=new TObjectIntHashMap<String>(100,0.8f,-1);
		int lineNum=0;
		List<String> keys=new ArrayList<String>();
		for(String line:lines){
			String prefix="p"+rnd.nextInt(10);
			String key=prefix+":"+line; 
			keys.add(key);
			fd.get(key, true);
			map.put(key, lineNum++);
		}
		//check
		if(fd.size()!=lines.size()){
			System.out.println("size not equal");
		}
		TObjectIntIterator<String> iter=fd.iterator();
		lineNum=0;
		while(iter.hasNext()){
			iter.advance();
			String k=iter.key();
			if(!map.containsKey(k)){
				System.out.println("not found: "+k);
			}
			int v=map.get(k);
			if(v!=iter.value()){
				System.out.println("value not equals");
			}
			lineNum++;
		}
		if(lineNum!=lines.size()){
			System.out.println("wrong");
		}
		lineNum=0;
		for(String key:keys){
			int v=fd.get(key, false);
			if(v==-1){
				System.out.println("key not found: "+key);
			}
			if(v!=lineNum){
				System.out.println("value not equal: "+key);
			}
			lineNum++;
		}
	}

}
