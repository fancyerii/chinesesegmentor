package com.antbrains.crf;

public interface TrainingProgress {
  public void startTraining();

  public void doIter(int iter);

  public void doValidate(String s);

  public void finishTraining();
}
