package com.antbrains.crf;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;

public class TroveFeatureDict implements FeatureDict {
  private static final long serialVersionUID = -8540035290826219173L;

  private TObjectIntHashMap<String> dict;

  @Override
  public int get(String feature, boolean addIfNotExist) {
    int id = dict.get(feature);
    if (id >= 0)
      return id;
    if (addIfNotExist) {
      id = dict.size();
      dict.put(feature, id);
    }
    return id;
  }

  public TroveFeatureDict(int initSize) {
    dict = new TObjectIntHashMap<String>(initSize, 0.8f, -1);
  }

  @Override
  public int size() {
    return dict.size();
  }

  @Override
  public TObjectIntIterator<String> iterator() {
    return dict.iterator();
  }
}
