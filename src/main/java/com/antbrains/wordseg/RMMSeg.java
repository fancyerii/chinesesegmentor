package com.antbrains.wordseg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;
import java.util.Map.Entry;

import com.antbrains.crf.hadoop.FileTools;
import com.antbrains.datrie.DoubleArrayTrie;

/**
 * 逆向最大匹配分词
 * 
 * @author lili
 *
 */
public class RMMSeg {
  private DoubleArrayTrie trie;

  private String reverseString(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = s.length() - 1; i >= 0; i--) {
      sb.append(s.charAt(i));
    }
    return sb.toString();
  }

  public RMMSeg(DoubleArrayTrie trie) {
    this.trie = trie;
  }

  public RMMSeg(List<String> wordList) {
    trie = new DoubleArrayTrie();
    for (String word : wordList) {
      word=word.trim();
      if(word.length()<2) continue;
      trie.coverInsert(reverseString(word), 0);
    }
  }

  public RMMSeg(String dictPath) {
    try {
      List<String> wordList = FileTools.readFile2List(dictPath, "UTF-8");
      for (String word : wordList) {
        trie.coverInsert(reverseString(word), 0);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String cnNumbers = "零一二三四五六七八九十百千万亿";
  public boolean processNumber = false;

  public List<Token> seg(String sentence) {
    String s = this.reverseString(sentence);
    List<Token> tokens = new ArrayList<Token>();
    Stack<Token> stack = new Stack<Token>();
    int length = sentence.length();

    for (int i = 0; i < s.length(); i++) {
      String ch = s.substring(i, i + 1);
      int len = trie.find(s, i)[0];
      if (processNumber && cnNumbers.contains(ch)) { // 处理汉字里的数字，阿拉伯数字在前面lucene的分析器已经处理过了
        int j = i + 1;
        for (; j < s.length(); j++) {
          ch = s.substring(j, j + 1);
          if (!cnNumbers.contains(ch))
            break;
        }
        if (j - i > 1 && j - i > len) {
          stack.add(new Token(sentence, length - j, length - i));
          i += (j - i - 1);
          continue;
        }
      }

      if (len > 1) {
        stack.push(new Token(sentence, length - i - len, length - i));
        i += (len - 1);
      } else {
        stack.push(new Token(sentence, length - i - 1, length - i));
      }
    }

    while (!stack.empty()) {
      tokens.add(stack.pop());
    }
    return tokens;
  }

}
