package com.antbrains.wordseg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.antbrains.crf.hadoop.FileTools;
import com.antbrains.datrie.DoubleArrayTrie;

/**
 * 正向最大匹配分词
 * 
 * @author lili
 *
 */
public class MMSeg {
  private DoubleArrayTrie trie;

  public MMSeg(DoubleArrayTrie trie) {
    this.trie = trie;
  }

  public MMSeg(List<String> wordList) {
    trie = new DoubleArrayTrie();
    for (String word : wordList) {
      trie.coverInsert(word, 0);
    }
  }

  public MMSeg(String dictPath) {
    try {
      List<String> wordList = FileTools.readFile2List(dictPath, "UTF-8");
      trie = new DoubleArrayTrie();
      for (String word : wordList) {
        word=word.trim();
        if(word.length()<2) continue;
        trie.coverInsert(word, 0);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String cnNumbers = "零一二三四五六七八九十百千万亿";
  public boolean processNumber = false;

  public List<Token> seg(String sen) {
    List<Token> tokens = new ArrayList<Token>();
    for (int i = 0; i < sen.length(); i++) {
      String ch = sen.substring(i, i + 1);
      int len = trie.find(sen, i)[0];
      if (processNumber && cnNumbers.contains(ch)) { // 处理汉字里的数字，阿拉伯数字在前面lucene的分析器已经处理过了
        int j = i + 1;
        for (; j < sen.length(); j++) {
          ch = sen.substring(j, j + 1);
          if (!cnNumbers.contains(ch))
            break;
        }
        if (j - i > 1 && j - i > len) {
          tokens.add(new Token(sen, i, j));
          i += (j - i - 1);
          continue;
        }

      }
      if (len > 1) {
        tokens.add(new Token(sen, i, i + len));
        i += (len - 1);
      } else {
        tokens.add(new Token(sen, i, i + 1));
      }

    }

    return tokens;
  }

}
