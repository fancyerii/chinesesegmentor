package com.antbrains.crf;

import java.util.List;

public interface TagConvertor extends java.io.Serializable {
  public String[] tokens2Tags(String[] tokens);

  public String[] tokens2Tags(List<String> tokens);

  public String[] tags2Tokens(String[] tags, String sentence);

  public List<String> tags2TokenList(String[] tags, String sentence);

  public int getTagNum();

  public List<String> getTags();
}
