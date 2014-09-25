package com.antbrains.crf.hadoop;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.antbrains.crf.BESB1B2MTagConvertor;
import com.antbrains.crf.CompactedTroveFeatureDict;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.TagConvertor;
import com.antbrains.crf.Template;
import com.antbrains.crf.TrainingParams;
import com.antbrains.crf.TrainingWeights;
import com.antbrains.crf.hadoop.ParallelTraining2.TrainingMapper;
import com.google.gson.Gson;

public class TruncateModel {

  public static void main(String[] args) throws Exception {
    if (args.length < 6) {
      System.out
          .println("Usage TruncateModel reverse_feature featureHDFSPath topN outPath confFile rawResult [conf1, conf2, ...]");
      System.exit(-1);
    }
    int topN = Integer.valueOf(args[2]);
    TrainingParams params = SgdCrf.loadParams(args[4]);
    System.out.println("Params: " + new Gson().toJson(params));
    Configuration conf = new Configuration();
    for (int i = 6; i < args.length; i++) {
      System.out.println("add resource: " + args[i]);
      conf.addResource(new Path(args[i]));
    }
    ArrayList<String> id2Feature = readFeature(args[0]);

    FileSystem fs = FileSystem.get(conf);
    Path inFile = new Path(args[1]);
    FileStatus[] status = fs.listStatus(inFile);

    TagConvertor tc = new BESB1B2MTagConvertor();

    TrainingWeights weights = new TrainingWeights(new Template(params.getTemplates().toArray(
        new String[0])));
    FeatureDict fd = new CompactedTroveFeatureDict(102400);
    weights.setAttributeDict(fd);
    TObjectIntHashMap<String> labelDict = new TObjectIntHashMap<String>();

    for (String tag : tc.getTags()) {
      labelDict.put(tag, labelDict.size());
    }
    weights.setLabelDict(labelDict);

    int labelNum = tc.getTagNum();
    weights.setAttributeWeights(new double[labelNum * topN]);
    weights.setTransitionWeights(new double[labelNum * labelNum]);
    weights.setBosTransitionWeights(new double[labelNum]);
    weights.setEosTransitionWeights(new double[labelNum]);
    String[] labelTexts = new String[labelNum];
    Iterator<String> iter = tc.getTags().iterator();
    for (int i = 0; i < labelNum; i++) {
      labelTexts[i] = iter.next();
    }
    weights.setLabelTexts(labelTexts);

    double[] fw = weights.getAttributeWeights();
    double[] bos = weights.getBosTransitionWeights();
    double[] eos = weights.getEosTransitionWeights();
    double[] tw = weights.getTransitionWeights();
    int fwCount = 0;
    int twCount = 0;
    int bosCount = 0;
    int eosCount = 0;
    int cur = 0;

    double maxScore = 0;
    double lastScore = Double.MAX_VALUE;
    for (FileStatus stat : status) {
      if (stat.isDir()) {

      } else {
        Path f = stat.getPath();
        if (f.getName().startsWith("_")) {
          System.out.println("ignore file: " + f.toString());
        } else {
          System.out.println(new java.util.Date() + " process: " + f.toString());

          SequenceFile.Reader reader = null;
          try {
            reader = new SequenceFile.Reader(fs, f, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value) && cur < topN) {
              MyKey k = (MyKey) key;
              MyValue v = (MyValue) value;
              String feature = id2Feature.get(k.id);
              int newId = fd.get(feature, true);
              if (cur == 0) {
                maxScore = k.score;
              }
              if (lastScore < k.score) {
                throw new Exception("score not desceding: " + cur + " lastScore: " + lastScore
                    + ", k.score: " + k.score);
              }

              lastScore = k.score;
              double[] scores = v.getArray();
              if (scores.length != labelNum) {
                throw new Exception("score.length!=labelNum: " + cur);
              }
              int startIdx = newId * labelNum;
              for (int i = 0; i < labelNum; i++) {
                fw[i + startIdx] = scores[i];
                fwCount++;
              }
              cur++;
            }
          } finally {
            if (reader != null) {
              reader.close();
            }
          }

        }
      }
    }

    System.out.println("fd.size: " + fd.size());
    id2Feature = null;
    System.out.println("reading other params");
    inFile = new Path(args[5]);
    status = fs.listStatus(inFile);

    for (FileStatus stat : status) {
      if (stat.isDir()) {

      } else {
        Path f = stat.getPath();
        if (f.getName().startsWith("_")) {
          System.out.println("ignore file: " + f.toString());
        } else {
          System.out.println(new java.util.Date() + " process: " + f.toString());

          SequenceFile.Reader reader = null;
          try {
            reader = new SequenceFile.Reader(fs, f, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
              Text k = (Text) key;
              DoubleWritable v = (DoubleWritable) value;
              String s = k.toString();
              if (s.startsWith(TrainingMapper.FEATURE_WEIGHT)) {

              } else if (s.startsWith(TrainingMapper.BOS_WEIGHT)) {
                int idx = Integer.parseInt(s.substring(2));
                bos[idx] = v.get();
                bosCount++;
              } else if (s.startsWith(TrainingMapper.EOS_WEIGHT)) {
                int idx = Integer.parseInt(s.substring(2));
                eos[idx] = v.get();
                eosCount++;
              } else if (s.startsWith(TrainingMapper.TRANSITION_WEIGHT)) {
                int idx = Integer.parseInt(s.substring(2));
                tw[idx] = v.get();
                twCount++;
              } else {
                throw new RuntimeException("unknown key: " + s);
              }
            }
          } finally {
            if (reader != null) {
              reader.close();
            }
          }

        }
      }
    }

    System.out.println("maxScore: " + maxScore);
    System.out.println("minScore: " + lastScore);
    System.out.println("fwCount: " + fwCount);
    System.out.println("twCount: " + twCount);
    System.out.println("bosCount: " + bosCount);
    System.out.println("eosCount: " + eosCount);

    SgdCrf.saveModel(params, weights, args[3]);

  }

  private static ArrayList<String> readFeature(String path) throws Exception {
    BufferedReader br = null;
    // List<String> lines=new ArrayList<String>();
    int lineNum = 0;
    try {
      String line;
      br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF8"));
      while ((line = br.readLine()) != null) {
        lineNum++;
      }
    } catch (Exception e) {
      throw e;
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException e) {
      }
    }
    System.out.println("total: " + lineNum);
    ArrayList<String> result = new ArrayList<String>(lineNum);
    for (int i = 0; i < lineNum; i++) {
      result.add(null);
    }
    try {
      String line;
      br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF8"));
      lineNum = 0;
      while ((line = br.readLine()) != null) {
        String[] arr = line.split("\t");
        int idx = Integer.valueOf(arr[1]);
        result.set(idx, arr[0]);

        lineNum++;
        if (lineNum % 100000 == 0) {
          System.out.println(new java.util.Date() + " " + lineNum);
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException e) {
      }
    }

    // check
    for (int i = 0; i < result.size(); i++) {
      String s = result.get(i);
      if (s == null) {
        throw new IOException(i + " is null");
      }
    }

    return result;
  }

}
