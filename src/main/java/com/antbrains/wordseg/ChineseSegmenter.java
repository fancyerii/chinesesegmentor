package com.antbrains.wordseg;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

 

import com.antbrains.crf.BESB1B2MTagConvertor;
import com.antbrains.crf.CrfModel;
import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.TagConvertor;
import com.antbrains.crf.hadoop.FileTools;
import com.antbrains.wordseg.Token.Type;
import com.antbrains.wordseg.luceneanalyzer.CharTermAttribute;
import com.antbrains.wordseg.luceneanalyzer.OffsetAttribute;
import com.antbrains.wordseg.luceneanalyzer.StandardTokenizer;
import com.antbrains.wordseg.luceneanalyzer.TypeAttribute;
import com.antbrains.wordseg.luceneanalyzer.Version;


public class ChineseSegmenter {
  private ChineseSegmenter(){
    try{
      InputStream is = ChineseSegmenter.class.getResourceAsStream("/crf.model");
      if(is==null) throw new RuntimeException("can't find /crf.model");
      model=SgdCrf.loadModel(is);
      is.close();
      is = ChineseSegmenter.class.getResourceAsStream("/segdict.txt");
      List<String> words=FileTools.read2List(is, "UTF8");  
      this.mmseg=new MMSeg(words);
      this.rmmseg=new RMMSeg(words);
    }catch(Exception e){
      
    }
  }
  
  private static ChineseSegmenter instance;
  static{
    instance=new ChineseSegmenter();
  }
  public static ChineseSegmenter getInstance(){
    return instance;
  }
  public MMSeg getMmseg() {
    return mmseg;
  }

  public RMMSeg getRmmseg() {
    return rmmseg;
  }

  private MMSeg mmseg;
  private RMMSeg rmmseg;
  private TagConvertor tc = new BESB1B2MTagConvertor();
  private CrfModel model;
  public CrfModel getModel() {
    return model;
  }

  public List<List<Token>> processByLuceneAnalyzer(String sen) {
    List<List<Token>> result = new ArrayList<List<Token>>();
    StandardTokenizer tokenizer = new StandardTokenizer(Version.LUCENE_29, new StringReader(sen));

    CharTermAttribute termAtt = (CharTermAttribute) tokenizer.addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = (OffsetAttribute) tokenizer.addAttribute(OffsetAttribute.class);
    TypeAttribute typeAtt = (TypeAttribute) tokenizer.addAttribute(TypeAttribute.class);

    List<Token> subSen = new ArrayList<Token>();
    try {
      int lastPos = 0;
      boolean lastIsCn = false;
      while (tokenizer.incrementToken()) {
        int start = offsetAtt.startOffset();
        int end = offsetAtt.endOffset();

        if (lastPos < start) {// 被StandardAnalyzer扔掉的都认为是标点，不用参与分词
          for (int i = lastPos; i < start; i++) {
            if (subSen.size() > 0) {
              result.add(subSen);
            }
            subSen = new ArrayList<Token>();
            subSen.add(new Token(null, sen, i, i + 1, Type.PUNCT));
          }
          lastIsCn = false;
        }
        lastPos = end;

        String wordType = typeAtt.type();

        Token token = new Token(sen, start, end);
        if (wordType.equals("<IDEOGRAPHIC>")) {// 汉字
          token.setType(Type.CWORD);
          if (!lastIsCn) {
            if (subSen.size() > 0) {
              result.add(subSen);
              subSen = new ArrayList<Token>();
            }
            lastIsCn = true;
          }
        } else {
          lastIsCn = false;
          if (subSen.size() > 0) {
            result.add(subSen);
          }
          subSen = new ArrayList<Token>();
          if (wordType.equals("<ALPHANUM>")) {
            token.setType(Type.ALPHA);
          } else if (wordType.equals("<NUM>")) {
            token.setType(Type.NUMBER);
          }
        }
        subSen.add(token);

      }
      if (subSen.size() > 0) {
        result.add(subSen);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  public List<Token> seg(String sen) {
    List<Token> result = new ArrayList<Token>();
    // 先用StandardAnalyzer处理一遍
    List<List<Token>> subSens = this.processByLuceneAnalyzer(sen);
    for (List<Token> subSen : subSens) {
      if (subSen.size() < 2) {
        result.addAll(subSen);
      } else {
        for (Token tk : this.segmentSentence(subSen)) {
          result.add(tk);

        }
      }
    }

    return result;
  }

  private String tokens2String(List<Token> tokens) {
    StringBuilder sb = new StringBuilder();
    for (Token tk : tokens) {
      sb.append(tk.getNormalizedText());
    }
    return sb.toString();
  }

  private boolean isEqual(List<Token> tks1, List<Token> tks2) {
    if (tks1.size() != tks2.size())
      return false;
    for (int i = 0; i < tks1.size(); i++) {
      if (tks1.get(i).getLength() != tks2.get(i).getLength()) {
        return false;
      }
    }

    return true;
  }

  private List<int[]> compareResult(List<Token> tks1, List<Token> tks2) {
    List<int[]> result = new ArrayList<int[]>();
    int i = 0;
    int j = 0;
    int pos1 = 0;
    int pos2 = 0;
    int lastAlignPos = 0;
    int lastAlignI = 0;
    int lastAlignJ = 0;

    while (true) {
      if (i >= tks1.size() || j >= tks2.size())
        break;
      Token tk1 = tks1.get(i);
      Token tk2 = tks2.get(j);
      if (tk1.getLength() == tk2.getLength()) {
        i++;
        pos1 += tk1.getLength();
        j++;
        pos2 += tk2.getLength();
        continue;
      }
      // 出现没对齐的情况
      lastAlignI = i;
      lastAlignJ = j;
      lastAlignPos = pos1;
      // i++;
      pos1 += tk1.getLength();
      // j++;
      pos2 += tk2.getLength();
      while (pos1 != pos2) {
        if (pos1 < pos2) {
          i++;
          // if(i==tks1.size()) break L;
          pos1 += tks1.get(i).getLength();
        } else {
          j++;
          // if(j==tks2.size()) break L;
          pos2 += tks2.get(j).getLength();
        }
      }
      if (pos1 != pos2)
        break;
      // 再次对齐
      result.add(new int[] { lastAlignPos, pos1, lastAlignI, lastAlignJ, i, j });
      i++;
      j++;
    }
    if (i < tks1.size() || j < tks2.size()) {
      System.err.println("Unexpected here ");
      for (Token tk : tks1) {
        System.out.print(tk.getOrigText() + " ");
      }
      System.out.println();
      for (Token tk : tks2) {
        System.out.print(tk.getOrigText() + " ");
      }
      System.out.println();

    }
    return result;
  }

  private List<Token> segmentSentence(List<Token> tokens) {
    // 首先用MMSeg和RMMSeg分词，如果不一致，就用CRFs消歧
    String s = this.tokens2String(tokens);

    List<Token> tks1 = this.mmseg.seg(s);
    List<Token> tks2 = this.rmmseg.seg(s);

    if (this.isEqual(tks1, tks2)) {
      return tks1;
    }

    List<int[]> diff = this.compareResult(tks1, tks2);
    List<Token> result = new ArrayList<Token>();
    int lastPos = 0;
    for (int[] arr : diff) {
      for (int i = lastPos; i < arr[2]; i++) {
        result.add(tks1.get(i));
      }

      boolean hasLeftContext = arr[2] > 0;
      boolean hasRightContext = arr[4] < tks1.size() - 2;
      if (hasLeftContext) {
        if (tks1.get(arr[2] - 1).getLength() != tks2.get(arr[3] - 1).getLength()) {
          hasLeftContext = false;
        }
      }
      if (hasRightContext) {
        if (tks1.get(arr[4] + 2).getLength() != tks2.get(arr[5] + 2).getLength()) {
          hasRightContext = false;
        }
      }
      int start1 = hasLeftContext ? arr[2] - 1 : arr[2];
      int end1 = hasRightContext ? arr[4] + 2 : arr[4] + 1;
      // 如果上文有歧义，暂时不考虑上下文，主要原因是实现起来有些繁琐，而且上下文影响不是很大
      // 比如：一千 年 来人 类 历史
      // 一 千年 来 人类 历史
      // 当考虑 “来人类” 的时候 ，左边的上下文是不确定的
      int start2 = hasLeftContext ? arr[3] - 1 : arr[3];
      int end2 = hasRightContext ? arr[5] + 2 : arr[5] + 1;

      List<Token> subList1 = tks1.subList(start1, end1);
      List<Token> subList2 = tks2.subList(start2, end2);
      

      double score1 = SgdCrf.getScore(this.token2Array(subList1), tc, model);
      double score2 = SgdCrf.getScore(this.token2Array(subList2), tc, model);
      if (score1 >= score2) {
        result.addAll(tks1.subList(arr[2], arr[4] + 1));
      } else {
        result.addAll(tks2.subList(arr[3], arr[5] + 1));
      }

      lastPos = arr[4] + 1;
    }
    for (int i = lastPos; i < tks1.size(); i++) {
      result.add(tks1.get(i));
    }
    return result;

  }
  
  
  private String[] token2Array(List<Token> tokens){
    String[] result=new String[tokens.size()];
    int i=0;
    for(Token token:tokens){
      result[i++]=token.getOrigText();
    }
    return result;
  }
}
