package com.antbrains.crf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class Instance implements Serializable, Writable {
  private static final long serialVersionUID = -87276062282755946L;
  private int[] attrIds; // item*feature_num
  private int[] labelIds; // labels of each items
  private int length; // number of items

  public Instance(int[] attrIds, int length) {
    if (attrIds == null) {
      throw new NullPointerException("attrIds");
    }
    this.attrIds = attrIds;
    this.length = length;
  }

  public Instance(int[] attrIds, int[] labelIds) {
    this(attrIds, labelIds.length);
    this.labelIds = labelIds;
  }

  public int[] getAttrIds() {
    return attrIds;
  }

  public void setAttrIds(int[] attrIds) {
    this.attrIds = attrIds;
  }

  public int[] labelIds() {
    return labelIds;
  }

  public int length() {
    return length;
  }

  /**
   * @return featrue_num
   */
  public int rowSize() {
    return attrIds.length / length;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.length = in.readInt();
    int attrLen = in.readInt();

    this.attrIds = new int[attrLen];

    for (int i = 0; i < attrLen; i++) {
      this.attrIds[i] = in.readInt();
    }
    int labelLen = in.readInt();
    if (labelLen > 0) {
      this.labelIds = new int[labelLen];
      for (int i = 0; i < labelLen; i++) {
        this.labelIds[i] = in.readInt();
      }
    } else {
      this.labelIds = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(length);
    out.write(attrIds.length);
    for (int attrId : attrIds) {
      out.writeInt(attrId);
    }
    if (this.labelIds == null || this.labelIds.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(this.labelIds.length);
      for (int labelId : labelIds) {
        out.writeInt(labelId);
      }
    }

  }
}