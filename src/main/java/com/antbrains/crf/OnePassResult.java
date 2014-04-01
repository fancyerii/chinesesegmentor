package com.antbrains.crf;

import java.util.ArrayList;
import java.util.List;

import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Not thread safe
 * @author lili
 *
 */
public class OnePassResult {
	private long totalNumber;
	public void setTotalNumber(long totalNumber) {
		this.totalNumber = totalNumber;
	}
	private ArrayList<Instance> instances;
	public ArrayList<Instance> getInstances() {
		return instances;
	}
	public long getTotalNumber() {
		return totalNumber;
	}
	private FeatureDict attributes;
	private TObjectIntHashMap<String> labels;
	
	public TObjectIntHashMap<String> getLabels() {
		return labels;
	}
	public FeatureDict getAttributes() {
		return attributes;
	}
	public OnePassResult(FeatureDictEnum dictType){
		if(dictType==FeatureDictEnum.DOUBLE_ARRAY_TRIE){
			attributes=new DATrieFeatureDict();
		}else{
			attributes=new TroveFeatureDict(102400);
			
		}
		labels=new TObjectIntHashMap<String>(10,0.75f,-1);
		instances=new ArrayList<Instance>();
	}
 
	
	public int getAttributeId(String attr,boolean addIfNotExist){
		return attributes.get(attr, addIfNotExist);
	}
}
