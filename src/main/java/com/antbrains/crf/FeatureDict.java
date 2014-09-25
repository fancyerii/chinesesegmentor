package com.antbrains.crf;

import gnu.trove.iterator.TObjectIntIterator;

public interface FeatureDict extends java.io.Serializable {
  public int get(String feature, boolean addIfNotExist);

  public int size();

  public TObjectIntIterator<String> iterator();

}
