package com.antbrains.crf;

import gnu.trove.iterator.TObjectIntIterator;

import com.antbrains.datrie.DatrieIterator;
import com.antbrains.datrie.DoubleArrayTrie;

public class DATrieFeatureDict implements FeatureDict{
	private static final long serialVersionUID = 6807591420833652562L;
	private DoubleArrayTrie datrie;
	@Override
	public int get(String feature, boolean addIfNotExist) {
		int[] arr=datrie.find(feature, 0);
		if(arr[0]>0){
			return arr[1];
		}
		if(addIfNotExist){
			int id=datrie.size();
			datrie.coverInsert(feature, id);
			return id;
		}
		return -1;
	}
	
	public DATrieFeatureDict(){
		datrie=new DoubleArrayTrie();
		datrie.setMultiplyExpanding(true);
		datrie.setMultiply(1.5);
	}

	@Override
	public int size() {
		return datrie.size();
	}

	@Override
	public TObjectIntIterator<String> iterator() {
		final DatrieIterator iter=datrie.iterator();
		return new TObjectIntIterator<String>() {

			@Override
			public void advance() {
				iter.next();
			}

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public String key() {
				return iter.key();
			}

			@Override
			public int setValue(int value) {
				return iter.setValue(value);
			}

			@Override
			public int value() {
				return iter.value();
			}
		};
 
	}
	
}
