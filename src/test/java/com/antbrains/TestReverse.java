package com.antbrains;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import gnu.trove.map.hash.TIntObjectHashMap;

public class TestReverse {

  public static void main(String[] args) throws Exception {
    TIntObjectHashMap<String> map = new TIntObjectHashMap<String>();
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
        "/home/lili/reverse"), "UTF8"));
    String line;
    while ((line = br.readLine()) != null) {
      String[] arr = line.split("\t", 2);
      map.put(Integer.valueOf(arr[1]), arr[0]);
    }

    int[] attrs = new int[] { 4247785, 5556208, 5883142, 5558290, 8947425, 5094759, -1, -1, -1,
        5228694, 5556208, 5883142, 8524984, 8947425, 9377895, -1, -1, -1, 5228694, 5556208,
        5883142, 8524984, 8947425, 9377895, -1, -1, -1, 5228694, 5556208, 5883142, 8524984,
        8947425, 9377895, -1, -1, -1, 5228694, 5556208, 9157461, 8524984, 1753354, 2183479, -1, -1,
        -1, 5228694, 8829510, 5883221, 1331588, 1731804, 9377901, -1, -1, -1, 8501238, 5556287,
        8830142, 1309786, 1443401, 5093556, -1, -1, -1, 5228773, 8501870, 982233, 1021439, 6979595,
        4492228, -1, -1, -1, 8173712, 654848, 2616200, 6557037, 760199, 9049180, -1, -1, -1,
        327005, 2289415, 1963441, 338269, 1734511, 531821, -1, -1, -1, 1961862, 1636022, 5556959,
        1312785, 4753523, 5751003, -1, -1, -1, 1308454, 5229445, 8502611, 4331855, 807434, 8154991,
        -1, -1, -1, 4901582, 8174453, 4248946, 385370, 9304277, 1578978, -1, 326944, 8501225 };

    for (int attr : attrs) {
      if (attr != -1) {
        String s = map.get(attr);
        if (s.startsWith("U01")) {
          System.out.println(s);
        }
      }
    }

    br.close();
  }

}
