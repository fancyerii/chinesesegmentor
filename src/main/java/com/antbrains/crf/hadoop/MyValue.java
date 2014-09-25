package com.antbrains.crf.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyValue implements Writable {
  private double[] array;

  public MyValue() {

  }

  public double[] getArray() {
    return array;
  }

  public MyValue(double[] array) {
    this.array = array;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int arrLen = in.readInt();
    array = new double[arrLen];
    for (int i = 0; i < arrLen; i++) {
      array[i] = in.readDouble();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(array.length);
    for (double v : array) {
      out.writeDouble(v);
    }
  }

}
