package com.antbrains.crf.hadoop;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.ReflectionUtils;

import com.antbrains.crf.BESB1B2MTagConvertor;
import com.antbrains.crf.CrfModel;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.TagConvertor;
import com.antbrains.crf.Template;
import com.antbrains.crf.TrainingParams;
import com.antbrains.crf.TrainingWeights;
import com.antbrains.crf.hadoop.ParallelTraining2.TrainingMapper;
import com.google.gson.Gson;

import de.ruedigermoeller.serialization.FSTObjectInput;

public class ReadTrainingResult {

  public static void main(String[] args) throws IOException {
    if (args.length < 5) {
      System.out
          .println("Usage ReadTrainingResult <hdfsIn> <confFile> <outModel> <attrNum> <featuredict> [conf1, conf2, ...]");
      System.exit(-1);
    }

    TrainingParams params = SgdCrf.loadParams(args[1]);
    System.out.println("Params: " + new Gson().toJson(params));
    Configuration conf = new Configuration();
    for (int i = 5; i < args.length; i++) {
      System.out.println("add resource: " + args[i]);
      conf.addResource(new Path(args[i]));
    }
    FSTObjectInput foi = null;
    FeatureDict fd = null;
    try {
      foi = new FSTObjectInput(new FileInputStream(args[4]));
      try {
        fd = (FeatureDict) foi.readObject();
        int size = fd.size();
        System.out.println("fd.size=" + size);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

    } finally {
      if (foi != null) {
        foi.close();
      }
    }
    FileSystem fs = FileSystem.get(conf);
    Path inFile = new Path(args[0]);
    FileStatus[] status = fs.listStatus(inFile);
    int attrNum = Integer.valueOf(args[3]);
    System.out.println("attrNum: " + attrNum);
    TagConvertor tc = new BESB1B2MTagConvertor();

    TrainingWeights weights = new TrainingWeights(new Template(params.getTemplates().toArray(
        new String[0])));
    weights.setAttributeDict(fd);
    TObjectIntHashMap<String> labelDict = new TObjectIntHashMap<String>();

    for (String tag : tc.getTags()) {
      labelDict.put(tag, labelDict.size());
    }
    weights.setLabelDict(labelDict);

    int labelNum = tc.getTagNum();
    weights.setAttributeWeights(new double[labelNum * attrNum]);
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
                int idx = Integer.parseInt(s.substring(2));
                fw[idx] = v.get();
                fwCount++;
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
    System.out.println("fwCount: " + fwCount);
    System.out.println("twCount: " + twCount);
    System.out.println("bosCount: " + bosCount);
    System.out.println("eosCount: " + eosCount);

    SgdCrf.saveModel(params, weights, args[2]);
  }

}
