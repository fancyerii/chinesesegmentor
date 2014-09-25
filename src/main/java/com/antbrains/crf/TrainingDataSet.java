package com.antbrains.crf;

import java.util.List;

public class TrainingDataSet {
  public List<Instance> getInstances() {
    return instances;
  }

  public void setInstances(List<Instance> instances) {
    this.instances = instances;
  }

  public int getAttributeNum() {
    return attributeNum;
  }

  public void setAttributeNum(int attributeNum) {
    this.attributeNum = attributeNum;
  }

  public int getLabelNum() {
    return labelNum;
  }

  public void setLabelNum(int labelNum) {
    this.labelNum = labelNum;
  }

  private List<Instance> instances;
  // attribute Number
  private int attributeNum;
  // label Number
  private int labelNum;
}
