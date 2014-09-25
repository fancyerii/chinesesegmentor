package com.antbrains.crf;

import java.util.List;

public class TrainingParams implements java.io.Serializable {
  private static final long serialVersionUID = 3986434753174847633L;

  public int getMinFeatureFreq() {
    return minFeatureFreq;
  }

  public void setMinFeatureFreq(int minFeatureFreq) {
    this.minFeatureFreq = minFeatureFreq;
  }

  public double getSigma() {
    return sigma;
  }

  public void setSigma(double sigma) {
    this.sigma = sigma;
  }

  public double getEta() {
    return eta;
  }

  public void setEta(double eta) {
    this.eta = eta;
  }

  public double getRate() {
    return rate;
  }

  public void setRate(double rate) {
    this.rate = rate;
  }

  public int getSamplesNum() {
    return samplesNum;
  }

  public void setSamplesNum(int samplesNum) {
    this.samplesNum = samplesNum;
  }

  public int getCandidatesNum() {
    return candidatesNum;
  }

  public void setCandidatesNum(int candidatesNum) {
    this.candidatesNum = candidatesNum;
  }

  public List<String> getTemplates() {
    return templates;
  }

  public void setTemplates(List<String> templates) {
    this.templates = templates;
  }

  public int getIterationNum() {
    return iterationNum;
  }

  public void setIterationNum(int iterationNum) {
    this.iterationNum = iterationNum;
  }

  private int minFeatureFreq; // threshold for feature, if a feature's freq is less than this value,
                              // it will be discarded. default 1
  private double sigma; // default 10.0
  private double eta; // default 0.1
  private double rate; // learning rate, default 2.0
  private int samplesNum; // number of samples in calibrate default 1000
  private int candidatesNum; // number of candidate in calibrate default 10
  private List<String> templates;
  private int iterationNum; // iteration number
  private double t0;

  public double getT0() {
    return t0;
  }

  public void setT0(double t0) {
    this.t0 = t0;
  }

}
