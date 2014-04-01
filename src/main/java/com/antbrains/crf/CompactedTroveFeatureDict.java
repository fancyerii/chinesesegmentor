package com.antbrains.crf;

import java.util.ArrayList;
import java.util.Iterator;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;

public class CompactedTroveFeatureDict implements FeatureDict {
	private static final long serialVersionUID = -8540035290826219173L;

	private ArrayList<Dict> dicts;
	private int size=0;
	private int initSize;
	@Override
	public int get(String feature, boolean addIfNotExist) {
		int idx=feature.indexOf(":");
		String prefix=feature.substring(0,idx);
		String key=feature.substring(idx+1);
		int i=0;
		Dict dict=null;
		for(;i<dicts.size();i++){
			Dict d=dicts.get(i);
			int cmp=prefix.compareTo(d.prefix);
			if(cmp==0){//exists
				dict=d;
				break;
			}else if(cmp<0){
				break;
			}
		}
		if(dict==null){
			if(addIfNotExist){
				dict=new Dict();
				dict.prefix=prefix;
				dict.map=new TObjectIntHashMap<String>(initSize,0.8f,-1);
				dicts.add(i, dict);
			}else{
				return -1;
			}
		}
 
		int id=dict.map.get(key);
		if(id>=0) return id;
		if(addIfNotExist){
			id=size;
			dict.map.put(key, id);
			size++;
		}
		return id;
	}
	
	public CompactedTroveFeatureDict(int initSize){
		dicts=new ArrayList<Dict>();
		this.initSize=initSize;
		//dict=new TObjectIntHashMap<String>(initSize,0.8f,-1);
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public TObjectIntIterator<String> iterator() {
		return new DictsIterator(this.dicts.iterator(),size);
	}
}

class Dict implements java.io.Serializable{
	private static final long serialVersionUID = -3985115922937639535L;
	public String prefix;
	public TObjectIntHashMap<String> map;
}

class DictsIterator implements TObjectIntIterator<String>{
	private Iterator<Dict> dictIter;
	private TObjectIntIterator<String> wordIter;
	private String curPrefix;
	private int curIdx=0;
	private int totalSize;
	public DictsIterator(Iterator<Dict> dictIter,int totalSize){
		this.dictIter=dictIter;
		this.totalSize=totalSize;
		if(totalSize>0){
			Dict d=dictIter.next();
			curPrefix=d.prefix;
			wordIter=d.map.iterator();
		}
	}
	@Override
	public void advance() {
		while(true){
			if(wordIter.hasNext()){
				wordIter.advance();
				curIdx++;
				return;
			}
			//we have called hasNext() before,
			//so we assume dictIter.next() is true
			Dict d=dictIter.next();
			curPrefix=d.prefix;
			wordIter=d.map.iterator();
		}
	}

	@Override
	public boolean hasNext() {
		return curIdx<totalSize;
	}

	@Override
	public void remove() {
		wordIter.remove();
	}

	@Override
	public String key() {
		return curPrefix+":"+wordIter.key();
		
	}

	@Override
	public int value() {
		return wordIter.value();
	}

	@Override
	public int setValue(int paramInt) {
		return wordIter.setValue(paramInt);
	}
	
}
