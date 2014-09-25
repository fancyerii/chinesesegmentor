package com.antbrains.wordseg;

public class Token {
  public static enum Type {
    UNKNOWN, ALPHA, NUMBER, PUNCT, WHITESPACE, CWORD,
  }

  private String normalizedText;
  private Type type;
  private String context;
  private int beginIndex;
  private int endIndex;
  private int posInc = 1;

  public int getPosInc() {
    return posInc;
  }

  public void setPosInc(int posInc) {
    this.posInc = posInc;
  }

  public Token(String context, int beginIndex, int endIndex) {
    this.context = context;
    this.beginIndex = beginIndex;
    this.endIndex = endIndex;
  }

  public Token(String text, String context, int beginIndex, int endIndex, Type attr) {
    this.normalizedText = text;
    this.context = context;
    this.beginIndex = beginIndex;
    this.endIndex = endIndex;
    this.type = attr;
  }

  public String getNormalizedText() {
    if (normalizedText == null) {
      normalizedText = context.substring(beginIndex, endIndex);
    }
    return normalizedText;
  }

  public String getOrigText() {
    return context.substring(beginIndex, endIndex);
  }

  public String getContext() {
    return context;
  }

  public int getBeginIndex() {
    return beginIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  public int getLength() {
    return endIndex - beginIndex;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  /**
   * 用于合并Token之用
   * 
   * @see com.qunar.nlp.chinese.ChineseSegmenter#segment
   */
  public void setEndIndex(int endIndex) {
    this.endIndex = endIndex;
  }

  @Override
  public String toString() {
    return getOrigText();
  }
}
