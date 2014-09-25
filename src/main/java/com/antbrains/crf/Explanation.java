package com.antbrains.crf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 解析CRF为什么会做出这样的分词
 * 
 * @author lili
 *
 */
public class Explanation {
  public double[] bosTransitionWeights;
  public double[] eosTransitionWeights;
  public double[] transitionWeights;
  public int[] bestTagIds;

  public FeatureWeightScore[][] details;

  public List<String> tokens;
  public String[] labelTexts;
}
