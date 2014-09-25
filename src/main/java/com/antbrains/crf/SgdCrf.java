package com.antbrains.crf;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.google.gson.Gson;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

/*
 * 
 * java port of crfsuite's sgd crf.
 * see https://github.com/chokkan/crfsuite/blob/143be5187863091c25de98e0349947ee2f98cd4f/lib/crf/src/train_pegasos.c
 * 
 SGD for L2-regularized MAP estimation.

 The iterative algorithm is inspired by Pegasos:

 Shai Shalev-Shwartz, Yoram Singer, and Nathan Srebro.
 Pegasos: Primal Estimated sub-GrAdient SOlver for SVM.
 In Proc. of ICML 2007, pp 807-814, 2007.

 The calibration strategy is inspired by the implementation of sgd:
 http://leon.bottou.org/projects/sgd
 written by Léon Bottou.

 The objective function to minimize is:

 f(w) = (lambda/2) * ||w||^2 + (1/N) * \sum_i^N log P^i(y|x)
 lambda = 2 * C / N

 The original version of the Pegasos algorithm.

 0) Initialization
 t = t0
 k = [the batch size]
 1) Computing the learning rate (eta).
 eta = 1 / (lambda * t)
 2) Updating feature weights.
 w = (1 - eta * lambda) w - (eta / k) \sum_i (oexp - mexp)
 3) Projecting feature weights within an L2-ball.
 w = min{1, (1/sqrt(lambda))/||w||} * w
 4) Goto 1 until convergence.

 A naive implementation requires O(K) computations for steps 2 and 3,
 where K is the total number of features. This code implements the procedure
 in an efficient way:

 0) Initialization
 norm2 = 0
 decay = 1
 proj = 1
 1) Computing various factors
 eta = 1 / (lambda * t)
 decay *= (1 - eta * lambda)
 scale = decay * proj
 gain = (eta / k) / scale
 2) Updating feature weights
 Updating feature weights from observation expectation:
 delta = gain * (1.0) * f(x,y)
 norm2 += delta * (delta + w + w);
 w += delta
 Updating feature weights from model expectation:
 delta = gain * (-P(y|x)) * f(x,y)
 norm2 += delta * (delta + w + w);
 w += delta
 3) Projecting feature weights within an L2-ball
 If 1.0 / lambda < norm2 * scale * scale:
 proj = 1.0 / (sqrt(norm2 * lambda) * scale)
 4) Goto 1 until convergence.
 */
public class SgdCrf {
  private static void initWeights(TrainingWeights model) {
    Arrays.fill(model.getBosTransitionWeights(), 0);
    Arrays.fill(model.getEosTransitionWeights(), 0);
    Arrays.fill(model.getTransitionWeights(), 0);
    Arrays.fill(model.getAttributeWeights(), 0);
  }

  private static double[] computeStateScores(Instance instance, boolean exp, int labelNum,
      double[] attributeWeights) {
    int itemNum = instance.length();
    int rowSize = instance.rowSize();
    int[] attrIds = instance.getAttrIds();
    double[] stateScores = new double[itemNum * labelNum];
    for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
      for (int i = 0; i < rowSize; i++) {
        int attributeIndex = attrIds[itemIndex * rowSize + i];
        if (attributeIndex >= 0) {
          for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
            stateScores[itemIndex * labelNum + labelIndex] += attributeWeights[attributeIndex
                * labelNum + labelIndex];
          }
        }
      }
    }
    if (exp) {
      for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
        for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
          double score = stateScores[itemIndex * labelNum + labelIndex];
          stateScores[itemIndex * labelNum + labelIndex] = Math.exp(score);
        }
      }
    }
    return stateScores;
  }

  private static double[][] computeForwardScores(Instance instance,
      double[] expBosTransitionWeights, double[] expEosTransitionWeights,
      double[][] expTransitionWeights, double[] expStatesScores, double[] scaleFactors, int labelNum) {
    int itemNum = instance.length();
    double[][] forwardScores = new double[itemNum][];
    for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
      forwardScores[itemIndex] = new double[labelNum];
    }

    double sum = 0;

    double[] previousScores;
    double[] curScores = forwardScores[0];

    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      sum += curScores[labelIndex] = expBosTransitionWeights[labelIndex]
          * expStatesScores[labelIndex];
    }

    scaleFactors[0] = sum != 0 ? 1.0 / sum : 1;
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      curScores[labelIndex] *= scaleFactors[0];
    }

    for (int itemIndex = 1; itemIndex < itemNum; itemIndex++) {
      sum = 0;
      previousScores = forwardScores[itemIndex - 1];
      curScores = forwardScores[itemIndex];
      for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
        double score = 0;
        for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
          score += previousScores[fromLabelIndex]
              * expTransitionWeights[fromLabelIndex][toLabelIndex];
        }
        sum += curScores[toLabelIndex] = score
            * expStatesScores[itemIndex * labelNum + toLabelIndex];
      }
      scaleFactors[itemIndex] = sum != 0 ? 1.0 / sum : 1;
      for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
        curScores[labelIndex] *= scaleFactors[itemIndex];
      }
    }

    sum = 0;
    curScores = forwardScores[itemNum - 1];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      sum += curScores[labelIndex] * expEosTransitionWeights[labelIndex];
    }
    scaleFactors[itemNum] = sum != 0 ? 1.0 / sum : 1;

    return forwardScores;
  }

  private static double computeLogProb(Instance instance, double[] statesScores,
      double[][] forwardScores, double[][] backwardScores, double[] scaleFactors, double logNorm,
      double[] transitionWeights, int labelNum) {
    int itemNum = instance.length();

    int[] labels = instance.labelIds();

    int lastLabel = labels[0], label;
    double[] curScores = forwardScores[0];
    double instanceLogProb = Math.log(curScores[lastLabel]) - Math.log(scaleFactors[0]);

    for (int itemIndex = 1; itemIndex < itemNum; itemIndex++) {
      label = labels[itemIndex];

      instanceLogProb += transitionWeights[lastLabel * labelNum + label];
      instanceLogProb += Math.log(statesScores[itemIndex * labelNum + label]);
      lastLabel = label;
    }

    curScores = backwardScores[itemNum - 1];
    instanceLogProb += Math.log(curScores[lastLabel]) - Math.log(scaleFactors[itemNum - 1]);

    instanceLogProb -= logNorm;

    return instanceLogProb;
  }

  private static double computeInitialLoglikelihood(TrainingDataSet dataSet, int samplesNum,
      double lambda, double[] attributeWeights, double[] transitionWeights) {
    int labelNum = dataSet.getLabelNum();
    double[] expBosTransitionWeights = new double[labelNum];
    double[] expEosTransitionWeights = new double[labelNum];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      expBosTransitionWeights[labelIndex] = 1;
      expEosTransitionWeights[labelIndex] = 1;
    }
    double[][] expTransitionWeights = new double[labelNum][];
    for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
      expTransitionWeights[fromLabelIndex] = new double[labelNum];
      for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
        expTransitionWeights[fromLabelIndex][toLabelIndex] = 1;
      }
    }
    double logp = 0;
    List<Instance> instances = dataSet.getInstances();
    for (int i = 0; i < samplesNum; i++) {
      Instance instance = instances.get(i);
      double[] stateScores = computeStateScores(instance, true, labelNum, attributeWeights);
      double[] scaleFactors = new double[instance.length() + 1];
      double[][] forwardScores = computeForwardScores(instance, expBosTransitionWeights,
          expEosTransitionWeights, expTransitionWeights, stateScores, scaleFactors, labelNum);
      double logNorm = 0;
      for (int itemIndex = 0; itemIndex <= instance.length(); itemIndex++) {
        logNorm -= Math.log(scaleFactors[itemIndex]);
      }
      double[][] backwardScores = computeBackwardScores(instance, expEosTransitionWeights,
          expTransitionWeights, stateScores, scaleFactors, labelNum);
      logp += computeLogProb(instance, stateScores, forwardScores, backwardScores, scaleFactors,
          logNorm, transitionWeights, labelNum);
    }
    double norm2 = 0;
    for (int attributeIndex = 0; attributeIndex < dataSet.getAttributeNum(); attributeIndex++) {
      for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
        double weight = attributeWeights[attributeIndex * labelNum + labelIndex];
        norm2 += weight * weight;
      }
    }
    return logp - 0.5 * lambda * norm2 * samplesNum;
  }

  private static double[][] computeBackwardScores(Instance instance,
      double[] expEosTransitionWeights, double[][] expTransitionScores, double[] expStatesScores,
      double[] scaleFactors, int labelNum) {
    int itemNum = instance.length();
    double[][] backwardScores = new double[itemNum][];
    for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
      backwardScores[itemIndex] = new double[labelNum];
    }
    double[] curScores = backwardScores[itemNum - 1];
    double scale = scaleFactors[itemNum - 1];
    double[] nextScores, tranScores;

    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      curScores[labelIndex] = expEosTransitionWeights[labelIndex] * scale;
    }
    for (int itemIndex = itemNum - 2; itemIndex >= 0; itemIndex--) {
      curScores = backwardScores[itemIndex];
      nextScores = backwardScores[itemIndex + 1];
      scale = scaleFactors[itemIndex];

      for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
        double score = 0;
        tranScores = expTransitionScores[fromLabelIndex];
        for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
          score += tranScores[toLabelIndex]
              * expStatesScores[(itemIndex + 1) * labelNum + toLabelIndex]
              * nextScores[toLabelIndex];
        }
        curScores[fromLabelIndex] = score * scale;
      }
    }
    return backwardScores;
  }

  private static double calibrateSgd(List<Instance> trainInstances, int seqNum, double t0,
      double lambda, int labelNum, double[] bosTransitionWeights, double[] eosTransitionWeights,
      double[] transitionWeights, double[] attributeWeights) {
    int t = 0;
    double decay = 1.0, proj = 1.0;
    double scale = 0;
    double logp = 0;

    double[] expBosTransitionWeights = new double[labelNum];
    double[] expEosTransitionWeights = new double[labelNum];
    double[][] expTransitionWeights = new double[labelNum][];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      expTransitionWeights[labelIndex] = new double[labelNum];
    }

    for (int i = 0; i < seqNum; i++) {
      Instance instances = trainInstances.get(i);
      int itemNum = instances.length();

      double eta = 1.0 / (lambda * (t0 + t));
      decay *= (1.0 - eta * lambda);
      scale = decay * proj;
      double gain = eta / scale;

      for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
        expBosTransitionWeights[labelIndex] = Math.exp(bosTransitionWeights[labelIndex]);
        expEosTransitionWeights[labelIndex] = Math.exp(eosTransitionWeights[labelIndex]);
      }
      for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
        for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
          expTransitionWeights[fromLabelIndex][toLabelIndex] = Math
              .exp(transitionWeights[fromLabelIndex * labelNum + toLabelIndex]);
        }
      }

      double[] statesScores = computeStateScores(instances, true, labelNum, attributeWeights);

      double[] scaleFactors = new double[itemNum + 1];
      double[][] forwardScores = computeForwardScores(instances, expBosTransitionWeights,
          expEosTransitionWeights, expTransitionWeights, statesScores, scaleFactors, labelNum);
      double[][] backwardScores = computeBackwardScores(instances, expEosTransitionWeights,
          expTransitionWeights, statesScores, scaleFactors, labelNum);

      double logNorm = 0;
      for (int itemIndex = 0; itemIndex <= itemNum; itemIndex++) {
        logNorm -= Math.log(scaleFactors[itemIndex]);
      }
      logp += computeLogProb(instances, statesScores, forwardScores, backwardScores, scaleFactors,
          logNorm, transitionWeights, labelNum);

      updateFeatureWeights(instances, expTransitionWeights, statesScores, forwardScores,
          backwardScores, scaleFactors, gain, labelNum, attributeWeights, bosTransitionWeights,
          eosTransitionWeights, transitionWeights);

      ++t;
    }

    return logp;
  }

  private static double updateWeight(double[] weights, int index, double diff) {
    double oldWeight = weights[index];
    weights[index] = oldWeight + diff;
    return diff * (diff + oldWeight * 2);
  }

  private static double updateWeight(double[] weights, int index, double gain, double prob,
      boolean isTarget) {
    double norm2diff = updateWeight(weights, index, -gain * prob);
    if (isTarget) {
      norm2diff += updateWeight(weights, index, gain);
    }
    return norm2diff;
  }

  private static double updateFeatureWeights(Instance instance, double[][] expTransitionScores,
      double[] statesScores, double[][] forwardScores, double[][] backwardScores,
      double[] scaleFactors, double gain, int labelNum, double[] attributeWeights,
      double[] bosTransitionWeights, double[] eosTransitionWeights, double[] transitionWeights) {

    int itemNum = instance.length();
    int rowSize = instance.rowSize();
    int[] attrIds = instance.getAttrIds();
    int[] labelIds = instance.labelIds();

    double norm2diff = 0;

    double[] fwd = forwardScores[0];
    double[] bwd = backwardScores[0];
    double coeff = scaleFactors[itemNum] / scaleFactors[0];
    double[] probs = new double[labelNum];
    for (int i = 0; i < labelNum; i++) {
      probs[i] = fwd[i] * bwd[i] * coeff;
    }

    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      norm2diff += updateWeight(bosTransitionWeights, labelIndex, gain, probs[labelIndex],
          labelIndex == labelIds[0]);
    }

    for (int i = 0; i < rowSize; i++) {
      int attributeIndex = attrIds[i];
      if (attributeIndex < 0) {
        continue;
      }

      for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
        norm2diff += updateWeight(attributeWeights, attributeIndex * labelNum + labelIndex, gain,
            probs[labelIndex], labelIndex == labelIds[0]);
      }
    }

    fwd = forwardScores[itemNum - 1];
    bwd = backwardScores[itemNum - 1];
    coeff = scaleFactors[itemNum] / scaleFactors[itemNum - 1];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      probs[labelIndex] = fwd[labelIndex] * bwd[labelIndex] * coeff;
    }

    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      norm2diff += updateWeight(eosTransitionWeights, labelIndex, gain, probs[labelIndex],
          labelIndex == labelIds[itemNum - 1]);
    }

    for (int i = (itemNum - 1) * rowSize; i < attrIds.length; i++) {
      int attributeIndex = attrIds[i];
      if (attributeIndex < 0) {
        continue;
      }
      for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
        norm2diff += updateWeight(attributeWeights, attributeIndex * labelNum + labelIndex, gain,
            probs[labelIndex], labelIndex == labelIds[itemNum - 1]);
      }
    }

    for (int itemIndex = 1; itemIndex < itemNum - 1; itemIndex++) {
      fwd = forwardScores[itemIndex];
      bwd = backwardScores[itemIndex];
      coeff = scaleFactors[itemNum] / scaleFactors[itemIndex];

      for (int i = 0; i < labelNum; i++) {
        probs[i] = -1;
      }

      for (int i = 0; i < rowSize; i++) {
        int attributeIndex = attrIds[itemIndex * rowSize + i];
        if (attributeIndex < 0) {
          continue;
        }

        for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
          if (probs[labelIndex] == -1) {
            probs[labelIndex] = fwd[labelIndex] * bwd[labelIndex] * coeff;
          }
          norm2diff += updateWeight(attributeWeights, attributeIndex * labelNum + labelIndex, gain,
              probs[labelIndex], labelIndex == labelIds[itemIndex]);
        }
      }
    }

    for (int itemIndex = 0; itemIndex < itemNum - 1; itemIndex++) {
      fwd = forwardScores[itemIndex];
      bwd = backwardScores[itemIndex + 1];
      coeff = scaleFactors[itemNum];

      for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
        double[] edge = expTransitionScores[fromLabelIndex];
        for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
          norm2diff += updateWeight(transitionWeights, fromLabelIndex * labelNum + toLabelIndex,
              gain, fwd[fromLabelIndex] * edge[toLabelIndex]
                  * statesScores[(itemIndex + 1) * labelNum + toLabelIndex] * bwd[toLabelIndex]
                  * coeff, fromLabelIndex == labelIds[itemIndex]
                  && toLabelIndex == labelIds[itemIndex + 1]);
        }
      }
    }
    return norm2diff;
  }

  private static double calibrate(TrainingDataSet dataSet, double lambda, TrainingParams param,
      TrainingWeights weights) {
    List<Instance> instances = dataSet.getInstances();
    int samplesNum = Math.min(instances.size(), param.getSamplesNum());
    System.out.println(String.format("sgd.calibration.eta: %f\n", param.getEta()));
    System.out.println(String.format("sgd.calibration.rate: %f\n", param.getRate()));
    System.out.println(String.format("sgd.calibration.samples: %d\n", samplesNum));
    System.out.println(String.format("sgd.calibration.candidates: %d\n", param.getCandidatesNum()));

    Collections.shuffle(dataSet.getInstances());

    initWeights(weights);

    double initialLogProb = computeInitialLoglikelihood(dataSet, samplesNum, lambda,
        weights.getAttributeWeights(), weights.getTransitionWeights());
    System.out.println(String.format("Initial Log-likelihood: %f\n", initialLogProb));

    boolean decrease = false;
    int numOfCandidates = param.getCandidatesNum();
    int trial = 0;
    double bestLogp = -Double.MAX_VALUE;
    double initEtaValue = param.getEta();
    double bestEtaValue = initEtaValue;
    double etaValue = initEtaValue;

    System.out.println("calibrating");
    while (numOfCandidates > 0 || !decrease) {
      System.out.println(String.format("Trial #%d (eta = %f): ", trial + 1, etaValue));
      initWeights(weights);
      double logp = calibrateSgd(instances, samplesNum, 1.0 / (lambda * etaValue), lambda,
          dataSet.getLabelNum(), weights.getBosTransitionWeights(),
          weights.getEosTransitionWeights(), weights.getTransitionWeights(),
          weights.getAttributeWeights());
      boolean ok = !Double.isInfinite(logp) && logp > initialLogProb;
      if (ok) {
        System.out.println(String.format("%f\n", logp));
      } else {
        System.out.println(String.format("%f (worse)\n", logp));
      }
      if (ok) {
        numOfCandidates--;
        if (logp > bestLogp) {
          bestLogp = logp;
          bestEtaValue = etaValue;
        }
      }
      if (!decrease) {
        if (ok) {
          System.out.println(String.format("etaValue(%f)*=rate(%f)", etaValue, param.getRate()));
          etaValue *= param.getRate();
          System.out.println("etaValue=" + etaValue);
        } else {
          decrease = true;
          System.out.println(String.format("initEtaValue(%f)/=rate(%f)", initEtaValue,
              param.getRate()));
          etaValue = initEtaValue / param.getRate();
          System.out.println("etaValue=" + etaValue);
        }
      } else {
        System.out.println(String.format("etaValue(%f)/=rate(%f)", etaValue, param.getRate()));
        etaValue /= param.getRate();
        System.out.println("etaValue=" + etaValue);
      }
      ++trial;

    }
    etaValue = bestEtaValue;
    System.out.println(String.format("Best learning rate (eta): %f\n", etaValue));

    return 1.0 / (lambda * etaValue);
  }

  public static void saveModel(TrainingParams params, TrainingWeights weights, String fileName)
      throws IOException {
    FSTObjectOutput foo = null;
    try {
      foo = new FSTObjectOutput(new FileOutputStream(fileName));
      foo.writeObject(params, TrainingParams.class);
      foo.writeObject(weights, TrainingWeights.class);
    } finally {
      if (foo != null) {
        foo.close();
      }
    }

  }

  public static CrfModel loadModel(InputStream is) throws Exception {
    FSTObjectInput foi = null;
    try {
      foi = new FSTObjectInput(is);
      TrainingParams params = (TrainingParams) foi.readObject(TrainingParams.class);
      TrainingWeights weights = (TrainingWeights) foi.readObject(TrainingWeights.class);

      return new CrfModel(params, weights);
    } finally {
      if (foi != null) {
        foi.close();
      }
    }
  }

  public static CrfModel loadModel(String fileName) throws Exception {
    FSTObjectInput foi = null;
    try {
      foi = new FSTObjectInput(new FileInputStream(fileName));
      TrainingParams params = (TrainingParams) foi.readObject(TrainingParams.class);
      TrainingWeights weights = (TrainingWeights) foi.readObject(TrainingWeights.class);

      return new CrfModel(params, weights);
    } finally {
      if (foi != null) {
        foi.close();
      }
    }
  }

  private static Instance readInstance(Gson gson, String line) {
    String[] arr = line.split("\t", 2);
    return gson.fromJson(arr[1], Instance.class);
  }

  public static List<Instance> buildSamples4Calibrate(FileSystem hdfsFs, String path, int K,
      String charset, long[] totalSample) throws IOException {
    Instance[] r = new Instance[K];
    Path file = new Path(path);
    FileStatus[] status = hdfsFs.listStatus(file);
    RandomDataGenerator rndgen = new RandomDataGenerator();
    CompressionCodecFactory factory = new CompressionCodecFactory(hdfsFs.getConf());
    Gson gson = new Gson();
    long lineNum = 0;

    for (FileStatus stat : status) {
      if (stat.isDir()) {
        System.out.println("ignore subdir: " + stat.getPath().toString());
      } else {
        Path f = stat.getPath();
        if (f.getName().startsWith("_")) {
          System.out.println("ignore hidden file: " + f.toString());
        } else {
          System.out.println("process: " + f.toString());
          BufferedReader br = null;
          try {
            CompressionCodec codec = factory.getCodec(f);
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
              stream = codec.createInputStream(hdfsFs.open(f));
            } else {
              stream = hdfsFs.open(f);
            }
            br = new BufferedReader(new InputStreamReader(stream, charset));
            String line;
            while ((line = br.readLine()) != null) {
              Instance instance = null;

              // Reservoir sampling
              if (lineNum < K) {
                instance = readInstance(gson, line);
                r[(int) lineNum] = instance;
              } else {
                long j = rndgen.nextLong(0, lineNum);
                if (j < K) {
                  instance = readInstance(gson, line);
                  r[(int) j] = instance;
                }
              }

              lineNum++;
              if (lineNum % 100000 == 0) {
                System.out.println(new java.util.Date() + " buildSamples4Calibrate: " + lineNum);
              }
            }
          } finally {
            if (br != null) {
              br.close();
            }
          }
        }
      }
    }

    totalSample[0] = lineNum;

    List<Instance> instances = new ArrayList<Instance>(r.length);
    for (Instance instance : r) {
      if (instance != null) {
        instances.add(instance);
      }
    }

    return instances;
  }

  public static OnePassResult firstPassScan(FileSystem hdfsFs, String path, TagConvertor tc,
      Template template, int K, String charset, FeatureDictEnum dictType) throws IOException {
    OnePassResult opr = new OnePassResult(dictType);
    FeatureDict attributeDict = opr.getAttributes();
    TObjectIntHashMap<String> labelDict = opr.getLabels();
    Path file = new Path(path);
    FileStatus[] status = hdfsFs.listStatus(file);
    Random rnd = new Random();
    Instance[] r = new Instance[K];
    long lineNum = 0;
    for (FileStatus stat : status) {
      if (stat.isDir()) {
        System.out.println("ignore subdir: " + stat.getPath().toString());
      } else {
        Path f = stat.getPath();
        if (f.getName().startsWith(".")) {
          System.out.println("ignore hidden file: " + f.toString());
        } else {
          System.out.println("process: " + f.toString());
          BufferedReader br = null;
          try {
            br = new BufferedReader(new InputStreamReader(hdfsFs.open(f), charset));
            String line;
            while ((line = br.readLine()) != null) {
              String[] tokens = line.split("\t");
              Instance instance = buildInstance(tokens, tc, attributeDict, labelDict, template,
                  true, true);

              // Reservoir sampling
              if (lineNum < K) {
                r[(int) lineNum] = instance;
              } else {
                long j = rnd.nextLong() % lineNum;
                if (j < K) {
                  r[(int) j] = instance;
                }
              }

              lineNum++;
              if (lineNum % 100000 == 0) {
                System.out.println(new java.util.Date() + " firstPass: " + lineNum);
              }
            }
          } finally {
            if (br != null) {
              br.close();
            }
          }
        }
      }
    }
    opr.setTotalNumber(lineNum);
    ArrayList<Instance> instances = opr.getInstances();
    instances.ensureCapacity(K);
    for (Instance instance : r) {
      instances.add(instance);
    }
    return opr;
  }

  public static void genFeatureDictAndInstances(FileSystem hdfsFs, String path, int validateNum,
      Template template, int iterationNum, TrainingParams param, TagConvertor tc, String charset,
      TrainingWeights weights, FeatureDictEnum dictType, String featureFile, String instanceFile)
      throws IOException {
    int sampleNum = param.getSamplesNum();
    OnePassResult opr = firstPassScan(hdfsFs, path, tc, template, sampleNum, charset, dictType);
    FeatureDict attributeDict = opr.getAttributes();
    System.out.println("save Dict to " + featureFile);
    FSTObjectOutput foo = null;
    try {
      foo = new FSTObjectOutput(new FileOutputStream(featureFile));
      foo.writeObject(attributeDict);
    } finally {
      if (foo != null) {
        foo.close();
      }
    }
    System.out.println("save instance to " + instanceFile);
    List<Instance> instances = opr.getInstances();

    try {
      foo = new FSTObjectOutput(new FileOutputStream(instanceFile));
      foo.writeObject(instances);
    } finally {
      if (foo != null) {
        foo.close();
      }
    }
  }

  public static void train(FileSystem hdfsFs, String path, Template template, int iterationNum,
      TrainingParams param, TagConvertor tc, String charset, TrainingWeights weights,
      FeatureDict dict) throws IOException {
    int sampleNum = param.getSamplesNum();
    System.out.println("get instances for calibrate, number=" + sampleNum);
    TObjectIntHashMap<String> labelDict = new TObjectIntHashMap<String>();
    for (String tag : tc.getTags()) {
      labelDict.put(tag, labelDict.size());
    }
    long[] totalInstances = new long[0];
    List<Instance> calibrateInstances = buildSamples4Calibrate(hdfsFs, path, sampleNum, charset,
        totalInstances);
    System.out.println("totalInstances: " + totalInstances[0]);
    TrainingDataSet ds = new TrainingDataSet();
    ds.setLabelNum(labelDict.size());
    ds.setInstances(calibrateInstances);

    System.out.println("start sgd");

    int t = 0;
    double decay = 1.0, proj = 1.0;

    double lambda = 1.0 / (param.getSigma() * param.getSigma() * totalInstances[0]);

    int labelNum = labelDict.size();
    double[] expBosTransitionWeights = new double[labelNum];
    double[] expEosTransitionWeights = new double[labelNum];
    double[][] expTransitionWeighs = new double[labelNum][];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      expTransitionWeighs[labelIndex] = new double[labelNum];
    }

    System.out.println("calibrating");
    double t0 = calibrate(ds, lambda, param, weights);
    System.out.println(t0);
  }

  public static void train(TrainingDataSet dataSet, int validateNum, int iterationNum,
      TrainingParams param, TrainingWeights weights, TrainingProgress tp) {
    List<Instance> instances = dataSet.getInstances();
    int labelNum = dataSet.getLabelNum();

    Collections.shuffle(instances);
    List<Instance> validateInstances = instances.subList(0, validateNum);
    List<Instance> trainInstances = instances.subList(validateNum, instances.size());

    // System.out.println("start sgd");
    tp.startTraining();
    boolean validate = validateInstances != null && validateInstances.size() > 0;

    int t = 0;
    double decay = 1.0, proj = 1.0;
    int trainInstanceNum = trainInstances.size();

    double lambda = 1.0 / (param.getSigma() * param.getSigma() * trainInstanceNum);

    double[] expBosTransitionWeights = new double[labelNum];
    double[] expEosTransitionWeights = new double[labelNum];
    double[][] expTransitionWeighs = new double[labelNum][];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      expTransitionWeighs[labelIndex] = new double[labelNum];
    }
    double t0 = 0;
    if (param.getT0() == 0) {
      // System.out.println("calibrating");
      t0 = calibrate(dataSet, lambda, param, weights);
    } else {
      t0 = param.getT0();
    }

    double norm2 = 0;

    initWeights(weights);
    double[] bosTransitionWeights = weights.getBosTransitionWeights();
    double[] eosTransitionWeights = weights.getEosTransitionWeights();
    double[] transitionWeights = weights.getTransitionWeights();
    double[] attributeWeights = weights.getAttributeWeights();
    // int attributeNum = attributeWeights.length;

    for (int epoch = 1; epoch <= iterationNum; epoch++) {
      tp.doIter(epoch);
      // System.out.println(new java.util.Date()+" iteration No. " +
      // epoch);
      Collections.shuffle(trainInstances);

      double scale = 0;

      for (Instance trainInstance : trainInstances) {

        double eta = 1.0 / (lambda * (t0 + t));
        decay *= (1.0 - eta * lambda);
        scale = decay * proj;
        double gain = eta / scale;

        for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
          expBosTransitionWeights[labelIndex] = Math.exp(bosTransitionWeights[labelIndex]);
          expEosTransitionWeights[labelIndex] = Math.exp(eosTransitionWeights[labelIndex]);
        }
        for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
          for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
            expTransitionWeighs[fromLabelIndex][toLabelIndex] = Math
                .exp(transitionWeights[fromLabelIndex * labelNum + toLabelIndex]);
          }
        }

        int itemNum = trainInstance.length();
        double[] statesScores = computeStateScores(trainInstance, true, labelNum, attributeWeights);
        double[] scaleFactors = new double[itemNum + 1];

        double[][] forwardScores = computeForwardScores(trainInstance, expBosTransitionWeights,
            expEosTransitionWeights, expTransitionWeighs, statesScores, scaleFactors, labelNum);
        double[][] backwardScores = computeBackwardScores(trainInstance, expEosTransitionWeights,
            expTransitionWeighs, statesScores, scaleFactors, labelNum);

        norm2 += updateFeatureWeights(trainInstance, expTransitionWeighs, statesScores,
            forwardScores, backwardScores, scaleFactors, gain, labelNum, attributeWeights,
            bosTransitionWeights, eosTransitionWeights, transitionWeights);

        double boundary = norm2 * scale * scale * lambda;
        if (boundary > 1.0) {
          proj = 1.0 / Math.sqrt(boundary);
        }
        ++t;

      }

      if (scale < 1e-20) {
        for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
          bosTransitionWeights[labelIndex] *= scale;
          eosTransitionWeights[labelIndex] *= scale;
        }
        for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
          for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
            transitionWeights[fromLabelIndex * labelNum + toLabelIndex] *= scale;
          }
        }
        // for (int attributeIndex = 0; attributeIndex < attributeNum;
        // attributeIndex++) {
        // for (int labelIndex = 0; labelIndex < labelNum; labelIndex++)
        // {
        // int idx=attributeIndex * labelNum + labelIndex;
        // if(idx==54655554){
        // throw new
        // RuntimeException(String.format("idx(%d)=attributeIndex(%d) * labelNum(%d) + labelIndex(%d)",idx,attributeIndex,labelNum,labelIndex));
        // }
        // attributeWeights[idx] *= scale;
        // }
        // }
        for (int idx = 0; idx < attributeWeights.length; idx++) {
          attributeWeights[idx] *= scale;
        }
        decay = 1.0;
        proj = 1.0;
        // System.out.println("scale weights and reset decay and proj to 1");
      }

      if (validate) {
        EvaluationResult statsOnValidateData = evaluate(validateInstances, weights);
        // System.out.println("statistics on validate data: ");
        // System.out.println("\n" + statsOnValidateData.toString() +
        // "\n");
        tp.doValidate(statsOnValidateData.toString());
      }

    }

    // System.out.println("validate on all data");
    EvaluationResult statsOnValidateData = evaluate(instances, weights);
    tp.doValidate(statsOnValidateData.toString());
    // System.out.println("statistics on validate data: ");
    // System.out.println("\n" + statsOnValidateData.toString() + "\n");
  }

  private static EvaluationResult evaluate(List<Instance> instances, TrainingWeights weights) {
    String[] labelTexts = weights.getLabelTexts();
    EvaluationResult evaluation = new EvaluationResult(labelTexts);
    for (Instance instance : instances) {
      int[] tagIds = tagId(instance, weights);
      evaluation.totalItemCount += tagIds.length;
      boolean hasError = false;
      for (int itemIndex = 0; itemIndex < instance.length(); itemIndex++) {
        int keyIndex = tagIds[itemIndex];
        int answerIndex = instance.labelIds()[itemIndex];
        for (int labelIndex = 0; labelIndex < labelTexts.length; labelIndex++) {
          int[] counts = evaluation.labelIndex2count[labelIndex];
          if (answerIndex == labelIndex) {
            if (keyIndex == labelIndex) {
              counts[EvaluationResult.TP_INDEX]++;
            } else {
              counts[EvaluationResult.FP_INDEX]++;
            }
          } else {
            if (keyIndex == labelIndex) {
              counts[EvaluationResult.FN_INDEX]++;
            } else {
              counts[EvaluationResult.TN_INDEX]++;
            }
          }
        }
        if (answerIndex == keyIndex) {
          evaluation.correctItemCount++;
        } else if (!hasError) {
          hasError = true;
        }
      }
      evaluation.totalSeqCount++;
      if (!hasError) {
        evaluation.correctSeqCount++;
      }
    }
    return evaluation;
  }
  
  public static double getScore(String[] tokens, TagConvertor tc, CrfModel model){
    TrainingWeights weights=model.weights;
    Instance instance = buildInstance(tokens, tc, weights.getAttributeDict(),
        weights.getLabelDict(), weights.getTemplate(), false, false);
    return getScore(weights, instance);
  }
  
  public static double getScore(TrainingWeights weights,Instance instance){
    int labelNum = weights.getLabelTexts().length;
    int[] tagIds=instance.labelIds();
    double[] stateScores = computeStateScores(instance, false, labelNum, weights.getAttributeWeights());
    double score=0;
    int labelIndex=tagIds[0];
    score=weights.getBosTransitionWeights()[labelIndex] + stateScores[labelIndex];
    
    for(int i=1;i<tagIds.length;i++){
        score+=(weights.getTransitionWeights()[tagIds[i-1]*labelNum+tagIds[i]]+stateScores[i*labelNum+tagIds[i]]);
    }
    
    score+=weights.getEosTransitionWeights()[tagIds[tagIds.length-1]];
    return score;
}

  public static int[] tagId(Instance instance, TrainingWeights weights) {
    if (instance == null) {
      return new int[0];
    }
    int itemNum = instance.length();
    int labelNum = weights.getLabelTexts().length;
    int[] tagIndexes = new int[itemNum];

    double[] stateScores = computeStateScores(instance, false, labelNum,
        weights.getAttributeWeights());

    int[] bestBackIndexes = new int[itemNum * labelNum];

    double[] bestScores = new double[itemNum * labelNum];
    double[] bosTransitionWeights = weights.getBosTransitionWeights();
    double[] transitionWeights = weights.getTransitionWeights();
    double[] eosTransitionWeights = weights.getEosTransitionWeights();
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      bestScores[labelIndex] = bosTransitionWeights[labelIndex] + stateScores[labelIndex];
    }

    for (int itemIndex = 1, itemMulIndex = labelNum; itemIndex < itemNum; itemIndex++, itemMulIndex += labelNum) {
      for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
        double maxScore = bestScores[itemMulIndex - labelNum] + transitionWeights[toLabelIndex];
        ;
        int maxFromLabelIndex = 0;
        for (int fromLabelIndex = 1, fromLabelMulIndex = labelNum; fromLabelIndex < labelNum; fromLabelIndex++, fromLabelMulIndex += labelNum) {
          double score = bestScores[itemMulIndex - labelNum + fromLabelIndex]
              + transitionWeights[fromLabelMulIndex + toLabelIndex];
          if (score > maxScore) {
            maxScore = score;
            maxFromLabelIndex = fromLabelIndex;
          }
        }
        bestScores[itemMulIndex + toLabelIndex] = maxScore
            + stateScores[itemMulIndex + toLabelIndex];
        bestBackIndexes[itemMulIndex + toLabelIndex] = maxFromLabelIndex;
      }
    }

    int itemMulIndex = (itemNum - 1) * labelNum;
    double maxScore = bestScores[itemMulIndex] + eosTransitionWeights[0];
    int maxFromLabelIndex = 0;
    for (int labelIndex = 1; labelIndex < labelNum; labelIndex++) {
      double score = bestScores[itemMulIndex + labelIndex] + eosTransitionWeights[labelIndex];
      if (score > maxScore) {
        maxScore = score;
        maxFromLabelIndex = labelIndex;
      }
    }
    tagIndexes[itemNum - 1] = maxFromLabelIndex;

    for (int itemIndex = itemNum - 2; itemIndex >= 0; itemIndex--, itemMulIndex -= labelNum) {
      maxFromLabelIndex = bestBackIndexes[itemMulIndex + maxFromLabelIndex];
      tagIndexes[itemIndex] = maxFromLabelIndex;
    }
    return tagIndexes;
  }

  private static int getFeatureIndex(FeatureDict map, String key, boolean isTraining) {
    int index = map.get(key, false);

    if (index < 0 && isTraining) {
      if (key.contains("_B-") || key.contains("_B+")) {
        return index;
      }
      index = map.get(key, true);

    }
    return index;
  }

  private static int getLabelIndex(TObjectIntHashMap<String> labelDict, String key,
      boolean isTraining) {
    int index = labelDict.get(key);
    if (index < 0 && isTraining) {
      labelDict.put(key, index = labelDict.size());
    }
    return index;
  }

  public static Instance buildInstance(String[] tokens, TagConvertor tc, FeatureDict attributeDict,
      TObjectIntHashMap<String> labelDict, Template template, boolean addFeatureIfNotExist,
      boolean addLableIfNotExist) {
    String[] tags = tc.tokens2Tags(tokens);
    int itemNum = tags.length;
    List<String> attributes = new ArrayList<String>();
    for (String token : tokens) {
      for (int i = 0; i < token.length(); i++) {
        attributes.add(token.substring(i, i + 1));
      }
    }
    if (template != null) {
      attributes = template.expandTemplate(attributes, itemNum);
    }
    int[] attrIds = new int[attributes.size()];
    int rowSize = attributes.size() / itemNum;
    int[] labelIds = new int[itemNum];

    for (int itemIndex = 0, attrIndex = 0; itemIndex < itemNum; itemIndex++) {
      labelIds[itemIndex] = getLabelIndex(labelDict, tags[itemIndex], addLableIfNotExist);
      // if(labelIds[itemIndex]==-1){
      // System.out.println("bug");
      // for(String token:tokens){
      // System.out.print(token+"\t");
      // }
      // System.out.println();
      // System.out.println("tags["+itemNum+"]="+tags[itemIndex]);
      // System.out.println(labelDict.size());
      // TObjectIntIterator<String> iter=labelDict.iterator();
      // while(iter.hasNext()){
      // iter.advance();
      // System.out.println(iter.key()+"\t"+iter.value());
      // }
      // System.exit(-1);
      // }
      for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        attrIds[attrIndex] = getFeatureIndex(attributeDict, attributes.get(attrIndex),
            addFeatureIfNotExist);
        attrIndex++;
      }
    }
    return new Instance(attrIds, labelIds);
  }

  private static Instance buildInstance(List<String> attributes, int itemNum, List<String> labels,
      TrainingWeights weights, boolean isTraining) {
    Template template = weights.getTemplate();
    TObjectIntHashMap<String> labelDict = weights.getLabelDict();
    FeatureDict attributeDict = weights.getAttributeDict();
    if (template != null) {
      attributes = template.expandTemplate(attributes, itemNum);
    }
    int[] attrIds = new int[attributes.size()];
    int rowSize = attributes.size() / itemNum;
    int[] labelIds = null;
    boolean containsLabels = labels != null;
    if (containsLabels) {
      labelIds = new int[itemNum];
    }
    for (int itemIndex = 0, attrIndex = 0; itemIndex < itemNum; itemIndex++) {
      if (containsLabels) {
        labelIds[itemIndex] = getLabelIndex(labelDict, labels.get(itemIndex), isTraining);
      }

      for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        attrIds[attrIndex] = getFeatureIndex(attributeDict, attributes.get(attrIndex), isTraining);
        attrIndex++;
      }
    }
    return containsLabels ? new Instance(attrIds, labelIds) : new Instance(attrIds, itemNum);
  }

  public static List<Instance> readTestData(String filename, String charset, TrainingWeights weights)
      throws IOException {
    return getInstances(filename, charset, true, weights, false);
  }

  public static List<Instance> readTestData2(String filename, String charset,
      TrainingWeights weights, TagConvertor tc) throws IOException {
    return getInstances2(filename, charset, true, weights, false, tc);
  }

  public static EvaluationResult readAndEvaluate(String filename, String charset,
      TrainingWeights weights, TagConvertor tc) throws IOException {
    BufferedReader br = null;

    String[] labelTexts = weights.getLabelTexts();
    EvaluationResult evaluation = new EvaluationResult(labelTexts);
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), charset));
      String line;
      int lineNumber = 0;
      while ((line = br.readLine()) != null) {
        if (line.trim().length() == 0) {
          continue;
        }
        lineNumber++;
        Instance instance = buildInstance(line.split("\t"), tc, weights.getAttributeDict(),
            weights.getLabelDict(), weights.getTemplate(), false, false);

        if (lineNumber % 10000 == 0) {
          System.out.println(lineNumber + " lines evaluated");
        }

        int[] tagIds = tagId(instance, weights);
        evaluation.totalItemCount += tagIds.length;
        boolean hasError = false;
        for (int itemIndex = 0; itemIndex < instance.length(); itemIndex++) {
          int keyIndex = tagIds[itemIndex];
          int answerIndex = instance.labelIds()[itemIndex];
          for (int labelIndex = 0; labelIndex < labelTexts.length; labelIndex++) {
            int[] counts = evaluation.labelIndex2count[labelIndex];
            if (answerIndex == labelIndex) {
              if (keyIndex == labelIndex) {
                counts[EvaluationResult.TP_INDEX]++;
              } else {
                counts[EvaluationResult.FP_INDEX]++;
              }
            } else {
              if (keyIndex == labelIndex) {
                counts[EvaluationResult.FN_INDEX]++;
              } else {
                counts[EvaluationResult.TN_INDEX]++;
              }
            }
          }
          if (answerIndex == keyIndex) {
            evaluation.correctItemCount++;
          } else if (!hasError) {
            hasError = true;
          }
        }
        evaluation.totalSeqCount++;
        if (!hasError) {
          evaluation.correctSeqCount++;
        }
      }

    } finally {
      if (br != null) {
        br.close();
      }
    }

    return evaluation;
  }

  private static TrainingDataSet shrinkAndInit(int minFeatureFreq, List<Instance> instances,
      TrainingWeights weights) {
    if (minFeatureFreq > 1) {
      SgdCrf.shrinkAttributeDict(instances, minFeatureFreq, weights.getAttributeDict());
    }
    TrainingDataSet dataSet = new TrainingDataSet();
    dataSet.setInstances(instances);
    int attrNum = weights.getAttributeDict().size();
    int labelNum = weights.getLabelDict().size();
    dataSet.setAttributeNum(attrNum);
    dataSet.setLabelNum(labelNum);

    weights.setAttributeWeights(new double[labelNum * attrNum]);
    weights.setTransitionWeights(new double[labelNum * labelNum]);
    weights.setBosTransitionWeights(new double[labelNum]);
    weights.setEosTransitionWeights(new double[labelNum]);

    final String[] labelTexts = new String[labelNum];
    weights.getLabelDict().forEachEntry(new gnu.trove.procedure.TObjectIntProcedure<String>() {
      @Override
      public boolean execute(String text, int index) {
        labelTexts[index] = text;
        return true;
      }
    });
    weights.setLabelTexts(labelTexts);
    return dataSet;
  }

  public static TrainingDataSet readTrainingData(String filename, String charset,
      TrainingWeights weights, int minFeatureFreq) throws IOException {
    List<Instance> instances = getInstances(filename, charset, true, weights, true);
    return shrinkAndInit(minFeatureFreq, instances, weights);

  }

  public static TrainingDataSet readTrainingData2(String filename, String charset,
      TrainingWeights weights, int minFeatureFreq, TagConvertor tc) throws IOException {
    TObjectIntHashMap<String> labeldict = weights.getLabelDict();
    for (String tag : tc.getTags()) {
      labeldict.put(tag, labeldict.size());
    }
    List<Instance> instances = getInstances2(filename, charset, true, weights, true, tc);
    return shrinkAndInit(minFeatureFreq, instances, weights);
  }

  private static List<Instance> getInstances2(String filename, String charset,
      boolean containsLabels, TrainingWeights weights, boolean isTraining, TagConvertor tc)
      throws IOException {

    BufferedReader br = null;
    List<Instance> instances = new ArrayList<Instance>();
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), charset));
      String line;
      int lineNumber = 0;
      while ((line = br.readLine()) != null) {
        if (line.trim().length() == 0) {
          continue;
        }
        lineNumber++;
        Instance instance = buildInstance(line.split("\t"), tc, weights.getAttributeDict(),
            weights.getLabelDict(), weights.getTemplate(), isTraining, false);
        instances.add(instance);
        if (lineNumber % 10000 == 0) {
          System.out.println(lineNumber + " lines read");
        }

      }

    } finally {
      if (br != null) {
        br.close();
      }
    }

    return instances;
  }

  private static List<Instance> getInstances(String filename, String charset,
      boolean containsLabels, TrainingWeights weights, boolean isTraining) throws IOException {
    BufferedReader br = null;
    List<Instance> instances = new ArrayList<Instance>();
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), charset));
      String line;
      List<String> itemList = new ArrayList<String>();
      List<String> labelList = containsLabels ? new ArrayList<String>() : null;

      int seqCount = 0, itemCount = 0;
      System.out.println("extracting instances");
      int fieldNum = -1;
      while ((line = br.readLine()) != null) {
        if (line.trim().length() == 0) {
          if (itemList.size() > 0) {
            Instance instance = buildInstance(itemList, labelList.size(), labelList, weights,
                isTraining);
            instances.add(instance);
            seqCount++;
            if (seqCount % 10000 == 0) {
              System.out.println(seqCount + " lines read");
            }
            itemList.clear();
            labelList.clear();
          }
        } else {
          String[] fields = line.split("\\s+");
          int thisFieldNum = containsLabels ? fields.length - 1 : fields.length;
          if (fieldNum < 0) {
            fieldNum = thisFieldNum;
          } else {
            if (fieldNum != thisFieldNum) {
              throw new IllegalStateException("inconsistent input format: " + line);
            }
          }
          if (containsLabels) {
            for (int i = 0; i < fields.length - 1; i++) {
              itemList.add(fields[i]);
            }
            labelList.add(fields[fields.length - 1]);
          } else {
            for (String field : fields) {
              itemList.add(field);
            }
          }
          itemCount++;
        }
      }
      if (itemList.size() > 0) {
        Instance instance = buildInstance(itemList, labelList.size(), labelList, weights,
            isTraining);
        instances.add(instance);
      }
      System.out.println("found " + seqCount + " instances and " + itemCount + " items");
    } finally {
      if (br != null) {
        br.close();
      }
    }

    return instances;
  }

  public static void showUsageAndExit() {
    System.err.println("Usage:");
    System.err.println("\t" + "SgdCrf help");
    System.err
        .println("\t"
            + "SgdCrf train <CRF++_format_train_file> <model_file> <crf_train_properties_file> [encoding]");
    System.err
        .println("\t"
            + "SgdCrf train2 <tab_sep_text_train_file> <model_file> <crf_train_properties_file> [encoding]");
    System.err
        .println("\t"
            + "SgdCrf hdfs-train <hdfs_dir> <model_file> <crf_train_properties_file> <feature_dict> [encoding] [hdfsconf1] [hdfsconf2] ...");
    System.err.println("\t" + "SgdCrf test  <test_file> <model_file> [encoding]");
    System.err.println("\t" + "SgdCrf test2  <test_file> <model_file> [encoding]");
    System.err.println("\t" + "SgdCrf tag <model_file> [nBest] [encoding]");
    System.exit(1);
  }

  public static TrainingParams loadParams(String configFile) throws IOException {
    TrainingParams params = new TrainingParams();
    Properties props = new Properties();
    props.load(new FileInputStream(new File(configFile)));

    params.setMinFeatureFreq(getIntParam(props, "mininumFeatureFrequency", 1));
    params.setEta(getDoubleParam(props, "eta", .1));
    params.setSigma(getDoubleParam(props, "sigma", 10.0));
    params.setRate(getDoubleParam(props, "rate", 2));
    params.setIterationNum(getIntParam(props, "iterateCount", 100));
    params.setCandidatesNum(getIntParam(props, "candidatesNum", 10));
    params.setSamplesNum(getIntParam(props, "samplesNum", 1000));
    params.setT0(getDoubleParam(props, "t0", 0));
    String templateFile = props.getProperty("templateFile");
    params.setTemplates(readTemplates(templateFile));
    return params;
  }

  public static List<String> readTemplates(String path) throws IOException {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
      List<String> lines = new ArrayList<String>();
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("#") || line.equals("")) {
          continue;
        }
        lines.add(line);
      }
      return lines;
    } finally {
      if (br != null) {
        br.close();
      }
    }
  }

  private static int shrinkAttributeDict(List<Instance> instances, int freqThreshold,
      FeatureDict attributeDict) {
    final int[] counter = new int[attributeDict.size()];
    for (Instance instance : instances) {
      int[] attrIds = instance.getAttrIds();
      for (int attrId : attrIds) {
        if (attrId >= 0) {
          counter[attrId]++;
        }
      }
    }
    int newNextAttrId = 0;
    for (int oldAttrId = 0; oldAttrId < counter.length; oldAttrId++) {
      if (counter[oldAttrId] > freqThreshold) {
        counter[oldAttrId] = newNextAttrId++;
      } else {
        counter[oldAttrId] = -1;
      }
    }

    TObjectIntIterator<String> iter = attributeDict.iterator();
    int removeNum = 0;
    while (iter.hasNext()) {
      iter.advance();
      int oldAttrId = iter.value();
      int newAttrId = counter[oldAttrId];
      if (newAttrId < 0) {
        iter.remove();
        removeNum++;
      } else {
        iter.setValue(newAttrId);
      }
    }

    for (Instance instance : instances) {
      int[] oldAttrIds = instance.getAttrIds();
      for (int i = 0; i < oldAttrIds.length; ++i) {
        int oldAttrId = oldAttrIds[i];
        if (oldAttrId >= 0) {
          oldAttrIds[i] = counter[oldAttrId];
        }
      }
    }
    return removeNum;
  }

  private static int getIntParam(Properties props, String key, int defaultValue) {
    if (props.containsKey(key)) {
      return Integer.valueOf(props.getProperty(key));
    } else {
      return defaultValue;
    }
  }

  private static double getDoubleParam(Properties props, String key, double defaultValue) {
    if (props.containsKey(key)) {
      return Double.valueOf(props.getProperty(key));
    } else {
      return defaultValue;
    }
  }

  private static Instance buildInstance4Explanation(List<String> attributes, int itemNum,
      List<String> labels, Map<Integer, String> featureMap, Template template,
      FeatureDict attributeDict, TObjectIntHashMap<String> labelDict) {
    if (template != null) {
      attributes = template.expandTemplate(attributes, itemNum);
    }
    int[] attrIds = new int[attributes.size()];
    int rowSize = attributes.size() / itemNum;
    int[] labelIds = null;
    boolean containsLabels = labels != null;
    if (containsLabels) {
      labelIds = new int[itemNum];
    }
    for (int itemIndex = 0, attrIndex = 0; itemIndex < itemNum; itemIndex++) {
      if (containsLabels) {
        labelIds[itemIndex] = getLabelIndex(labelDict, labels.get(itemIndex), false);
      }

      for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        attrIds[attrIndex] = getFeatureIndex(attributeDict, attributes.get(attrIndex), false);
        featureMap.put(attrIds[attrIndex], attributes.get(attrIndex));
        attrIndex++;
      }
    }
    return containsLabels ? new Instance(attrIds, labelIds) : new Instance(attrIds, itemNum);
  }

  private static double[] computeStateScores4Explanation(Instance instance, boolean exp,
      FeatureWeightScore[][] details, Map<Integer, String> featureMap, int labelNum,
      double[] attributeWeights) {
    int itemNum = instance.length();
    int rowSize = instance.rowSize();
    int[] attrIds = instance.getAttrIds();
    double[] stateScores = new double[itemNum * labelNum];

    for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
      for (int i = 0; i < rowSize; i++) {
        int attributeIndex = attrIds[itemIndex * rowSize + i];

        if (attributeIndex >= 0) {
          for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
            stateScores[itemIndex * labelNum + labelIndex] += attributeWeights[attributeIndex
                * labelNum + labelIndex];
            String feature = featureMap.get(attributeIndex);
            details[itemIndex][labelIndex].features.add(feature);
            details[itemIndex][labelIndex].weights.add(attributeWeights[attributeIndex * labelNum
                + labelIndex]);
          }
        }
      }
    }
    if (exp) {
      for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
        for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
          double score = stateScores[itemIndex * labelNum + labelIndex];
          stateScores[itemIndex * labelNum + labelIndex] = Math.exp(score);
        }
      }
    }
    for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
      for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
        details[itemIndex][labelIndex].score = stateScores[itemIndex * labelNum + labelIndex];
        List<String> featureList = details[itemIndex][labelIndex].features;
        List<Double> weightList = details[itemIndex][labelIndex].weights;
        List<Object[]> sortHelper = new ArrayList<Object[]>(featureList.size());
        for (int i = 0; i < featureList.size(); i++) {
          sortHelper.add(new Object[] { featureList.get(i), weightList.get(i) });
        }
        Collections.sort(sortHelper, new Comparator<Object[]>() {
          @Override
          public int compare(Object[] arg0, Object[] arg1) {
            double w1 = (Double) arg0[1];
            double w2 = (Double) arg1[1];
            w1 = Math.abs(w1);
            w2 = Math.abs(w2);
            if (w1 >= w2)
              return -1;
            else
              return 1;
          }

        });
        featureList.clear();
        weightList.clear();
        for (Object[] arr : sortHelper) {
          featureList.add((String) arr[0]);
          weightList.add((Double) arr[1]);
        }
      }
    }
    return stateScores;
  }

  public static Explanation explain(String sen, CrfModel model) {
    List<String> features = new ArrayList<String>(sen.length());
    for (int i = 0; i < sen.length(); i++) {
      features.add(sen.charAt(i) + "");
    }
    Explanation explanation = tagAndExplain(features, sen.length(), model);
    explanation.tokens = features;
    return explanation;
  }

  public static Explanation tagAndExplain(List<String> features, int itemNum, CrfModel model) {
    double[] bosTransitionWeights = model.weights.getBosTransitionWeights();
    double[] transitionWeights = model.weights.getTransitionWeights();
    double[] eosTransitionWeights = model.weights.getEosTransitionWeights();
    double[] attributeWeights = model.weights.getAttributeWeights();
    Template template = model.weights.getTemplate();
    FeatureDict attributeDict = model.weights.getAttributeDict();
    TObjectIntHashMap<String> labelDict = model.weights.getLabelDict();
    Explanation result = new Explanation();
    Map<Integer, String> featureMap = new HashMap<Integer, String>();
    Instance instance = buildInstance4Explanation(features, itemNum, null, featureMap, template,
        attributeDict, labelDict);

    // codes copied from tagId(Instance instance)
    if (instance == null) {
      result.bestTagIds = new int[0];
      return result;
    }

    int[] tagIndexes = new int[itemNum];
    int labelNum = model.weights.getLabelDict().size();
    FeatureWeightScore[][] details = new FeatureWeightScore[itemNum][];
    for (int i = 0; i < details.length; i++) {
      details[i] = new FeatureWeightScore[labelNum];
      for (int j = 0; j < details[i].length; j++) {
        details[i][j] = new FeatureWeightScore();
      }
    }
    result.details = details;
    double[] stateScores = computeStateScores4Explanation(instance, false, details, featureMap,
        labelNum, attributeWeights);

    int[] bestBackIndexes = new int[itemNum * labelNum];

    double[] bestScores = new double[itemNum * labelNum];
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      bestScores[labelIndex] = bosTransitionWeights[labelIndex] + stateScores[labelIndex];
    }

    for (int itemIndex = 1, itemMulIndex = labelNum; itemIndex < itemNum; itemIndex++, itemMulIndex += labelNum) {
      for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
        double maxScore = bestScores[itemMulIndex - labelNum] + transitionWeights[toLabelIndex];
        ;
        int maxFromLabelIndex = 0;
        for (int fromLabelIndex = 1, fromLabelMulIndex = labelNum; fromLabelIndex < labelNum; fromLabelIndex++, fromLabelMulIndex += labelNum) {
          double score = bestScores[itemMulIndex - labelNum + fromLabelIndex]
              + transitionWeights[fromLabelMulIndex + toLabelIndex];
          if (score > maxScore) {
            maxScore = score;
            maxFromLabelIndex = fromLabelIndex;
          }
        }
        bestScores[itemMulIndex + toLabelIndex] = maxScore
            + stateScores[itemMulIndex + toLabelIndex];
        bestBackIndexes[itemMulIndex + toLabelIndex] = maxFromLabelIndex;
      }
    }

    int itemMulIndex = (itemNum - 1) * labelNum;
    double maxScore = bestScores[itemMulIndex] + eosTransitionWeights[0];
    int maxFromLabelIndex = 0;
    for (int labelIndex = 1; labelIndex < labelNum; labelIndex++) {
      double score = bestScores[itemMulIndex + labelIndex] + eosTransitionWeights[labelIndex];
      if (score > maxScore) {
        maxScore = score;
        maxFromLabelIndex = labelIndex;
      }
    }
    tagIndexes[itemNum - 1] = maxFromLabelIndex;

    for (int itemIndex = itemNum - 2; itemIndex >= 0; itemIndex--, itemMulIndex -= labelNum) {
      maxFromLabelIndex = bestBackIndexes[itemMulIndex + maxFromLabelIndex];
      tagIndexes[itemIndex] = maxFromLabelIndex;
    }

    result.bestTagIds = tagIndexes;
    result.bosTransitionWeights = bosTransitionWeights;
    result.eosTransitionWeights = eosTransitionWeights;
    result.transitionWeights = transitionWeights;
    result.labelTexts = model.weights.getLabelTexts();
    return result;
  }

  public static List<String[]> tagNBest(Instance instance, int N, double[] relativeScore,
      CrfModel model) {
    List<String[]> result = new ArrayList<String[]>(N);

    if (instance == null) {
      return result;
    }
    int itemNum = instance.length();

    // compute state scores
    int labelNum = model.weights.getLabelDict().size();
    double[] attributeWeights = model.weights.getAttributeWeights();
    double[] bosTransitionWeights = model.weights.getBosTransitionWeights();
    double[] transitionWeights = model.weights.getTransitionWeights();
    double[] eosTransitionWeights = model.weights.getEosTransitionWeights();
    String[] labelTexts = model.weights.getLabelTexts();
    double[] stateScores = computeStateScores(instance, false, labelNum, attributeWeights);

    int[][][][] backs = new int[itemNum][][][];

    double[][][] scores = new double[itemNum][][];
    scores[0] = new double[labelNum][];
    for (int i = 0; i < labelNum; i++) {
      scores[0][i] = new double[N];
    }
    for (int labelIndex = 0; labelIndex < labelNum; labelIndex++) {
      scores[0][labelIndex][0] = stateScores[labelIndex] + bosTransitionWeights[labelIndex];
      for (int i = 1; i < N; i++) {
        scores[0][labelIndex][i] = -Double.MAX_VALUE;
      }
    }

    for (int itemIndex = 1; itemIndex < itemNum; itemIndex++) {
      scores[itemIndex] = new double[labelNum][];
      backs[itemIndex] = new int[labelNum][][];
      for (int toLabelIndex = 0; toLabelIndex < labelNum; toLabelIndex++) {
        // double maxScore = -Double.MAX_VALUE;
        // int maxFromLabelIndex = -1;
        double[] nBest = new double[N];
        int[][] nBestIndex = new int[N][2];
        int curCount = 0;
        nBest[0] = -Double.MAX_VALUE;

        for (int fromLabelIndex = 0; fromLabelIndex < labelNum; fromLabelIndex++) {
          for (int i = 0; i < N; i++) {
            if (scores[itemIndex - 1][fromLabelIndex][i] == -Double.MAX_VALUE) {
              break;
            }
            double score = scores[itemIndex - 1][fromLabelIndex][i]
                + transitionWeights[fromLabelIndex * labelNum + toLabelIndex];

            if (curCount < N || score > nBest[N - 1]) {
              int j = 0;
              for (; j < N; j++) {
                if (score > nBest[j])
                  break;
              }

              for (int k = N - 1; k > j; k--) {
                nBest[k] = nBest[k - 1];
                nBestIndex[k] = nBestIndex[k - 1];
              }
              nBest[j] = score;
              nBestIndex[j] = new int[] { fromLabelIndex, i };
              curCount++;
            }

          }

        }
        for (int i = 0; i < curCount && i < N; i++) {
          nBest[i] += stateScores[itemIndex * labelNum + toLabelIndex];
        }
        scores[itemIndex][toLabelIndex] = nBest;
        backs[itemIndex][toLabelIndex] = nBestIndex;
      }
    }

    List<Object[]> helper = new ArrayList<Object[]>();
    for (int i = 0; i < labelNum; i++) {
      double[] score = scores[itemNum - 1][i];
      for (int j = 0; j < N; j++) {
        double s = score[j];
        if (s == -Double.MAX_VALUE)
          break;
        s = s + eosTransitionWeights[i];

        Object[] arr = new Object[] { s, i, j };
        helper.add(arr);
      }
    }
    Collections.sort(helper, new Comparator<Object[]>() {
      @Override
      public int compare(Object[] arg0, Object[] arg1) {
        double s1 = (Double) arg0[0];
        double s2 = (Double) arg1[0];
        if (s1 >= s2)
          return -1;
        else
          return 1;
      }

    });

    int[] tmp;
    for (int i = 0; i < N; i++) {
      String[] tags = new String[itemNum];
      Object[] arr = helper.get(i);
      int j = (Integer) arr[1];
      int k = (Integer) arr[2];
      tags[itemNum - 1] = labelTexts[j];
      relativeScore[i] = (Double) arr[0];
      for (int itemIndex = itemNum - 2; itemIndex >= 0; itemIndex--) {
        tmp = backs[itemIndex + 1][j][k];
        j = tmp[0];
        k = tmp[1];
        tags[itemIndex] = labelTexts[j];
      }
      result.add(tags);
    }

    return result;
  }

  public static String[] tagId2Text(int[] tags, CrfModel model) {
    String[] labelTexts = model.weights.getLabelTexts();
    String[] tagTexts = new String[tags.length];
    for (int i = 0; i < tags.length; i++) {
      tagTexts[i] = labelTexts[tags[i]];
    }
    return tagTexts;
  }

  public static List<String> segment(String sentence, CrfModel model, TagConvertor tagConvertor) {
    List<String> attributes = new ArrayList<String>(sentence.length());
    for (int i = 0; i < sentence.length(); i++) {
      attributes.add(sentence.charAt(i) + "");
    }
    Instance instance = buildInstance(attributes, attributes.size(), null, model.weights, false);
    int[] tags = tagId(instance, model.weights);

    return tagConvertor.tags2TokenList(tagId2Text(tags, model), sentence);
  }

  public static List<String[]> segment(String sentence, CrfModel model, TagConvertor tagConvertor,
      int nBest) {
    List<String[]> result = new ArrayList<String[]>();
    List<String> attributes = new ArrayList<String>(sentence.length());
    for (int i = 0; i < sentence.length(); i++) {
      attributes.add(sentence.charAt(i) + "");
    }
    Instance instance = buildInstance(attributes, attributes.size(), null, model.weights, false);
    double[] relativeScore = new double[nBest];
    List<String[]> tags = tagNBest(instance, nBest, relativeScore, model);
    for (String[] tag : tags) {
      result.add(tagConvertor.tags2Tokens(tag, sentence));
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      showUsageAndExit();
    }
    String command = args[0];
    if (command.equals("help")) {
      showUsageAndExit();
    } else if (command.equals("train")) {
      if (args.length != 4 && args.length != 5) {
        showUsageAndExit();
      }
      String trainFilename = args[1];
      String modelFilename = args[2];
      String configFilename = args[3];

      String charset = "UTF8";
      if (args.length > 4) {
        charset = args[4];
      }

      TrainingParams params = loadParams(configFilename);
      Template template = new Template(params.getTemplates().toArray(new String[0]));
      TrainingWeights weights = new TrainingWeights(template, FeatureDictEnum.TROVE_HASHMAP);
      TrainingDataSet dataSet = SgdCrf.readTrainingData(trainFilename, charset, weights,
          params.getMinFeatureFreq());
      SgdCrf.train(dataSet, 0, params.getIterationNum(), params, weights,
          new PrintTrainingProgress());
      dataSet = null;// free memory
      SgdCrf.saveModel(params, weights, modelFilename);
      // SgdCrf.train(dataSet, validateNum, iterationNum, param, model);
    } else if (command.equals("train2")) {
      if (args.length != 4 && args.length != 5) {
        showUsageAndExit();
      }
      String trainFilename = args[1];
      String modelFilename = args[2];
      String configFilename = args[3];

      String charset = "UTF8";
      if (args.length > 4) {
        charset = args[4];
      }

      TrainingParams params = loadParams(configFilename);
      Template template = new Template(params.getTemplates().toArray(new String[0]));
      TrainingWeights weights = new TrainingWeights(template, FeatureDictEnum.TROVE_HASHMAP);
      TagConvertor tc = new BESB1B2MTagConvertor();
      TrainingDataSet dataSet = SgdCrf.readTrainingData2(trainFilename, charset, weights,
          params.getMinFeatureFreq(), tc);
      SgdCrf.train(dataSet, 0, params.getIterationNum(), params, weights,
          new PrintTrainingProgress());
      dataSet = null;
      SgdCrf.saveModel(params, weights, modelFilename);
    }
    // else if(command.equals("train-hdfs")){
    // if(!(args.length>=4)){
    // showUsageAndExit();
    // }
    // String trainFilename = args[1];
    // String modelFilename = args[2];
    // String configFilename = args[3];
    //
    // String charset = "UTF8";
    // if(args.length>4){
    // charset=args[4];
    // }
    //
    // Configuration conf = new Configuration();
    // for(int i=5;i<args.length;i++){
    // System.out.println("add hdfs conf: "+args[i]);
    // conf.addResource(new Path(args[i]));
    // }
    //
    // FileSystem fs = FileSystem.get(conf);
    // TrainingParams params=loadParams(configFilename);
    // Template template=new Template(params.getTemplates().toArray(new
    // String[0]));
    // TagConvertor tc=new BESB1B2MTagConvertor();
    // TrainingWeights weights=new
    // TrainingWeights(template,FeatureDictEnum.TROVE_HASHMAP);
    // SgdCrf.genFeatureDictAndInstances(fs, trainFilename, 0, template,
    // params.getIterationNum(), params, tc, charset, weights,
    // FeatureDictEnum.TROVE_HASHMAP, "./dict","./instances");
    //
    // }
    else if (command.equals("train-hdfs")) {
      if (!(args.length >= 5)) {
        showUsageAndExit();
      }
      String trainFilename = args[1];
      String modelFilename = args[2];
      String configFilename = args[3];
      String featureFilename = args[4];

      String charset = "UTF8";
      if (args.length > 5) {
        charset = args[5];
      }

      Configuration conf = new Configuration();
      for (int i = 6; i < args.length; i++) {
        System.out.println("add hdfs conf: " + args[i]);
        conf.addResource(new Path(args[i]));
      }

      FileSystem fs = FileSystem.get(conf);
      TrainingParams params = loadParams(configFilename);
      Template template = new Template(params.getTemplates().toArray(new String[0]));
      TagConvertor tc = new BESB1B2MTagConvertor();
      TrainingWeights weights = new TrainingWeights(template);
      // SgdCrf.train(fs, trainFilename, 0, template,
      // params.getIterationNum(), params, tc, charset, weights,
      // FeatureDictEnum.TROVE_HASHMAP, "./dict","./instances");
      FSTObjectInput foi = null;
      FeatureDict dict = null;
      System.out.println("load featuredict from: " + featureFilename);
      try {
        foi = new FSTObjectInput(new FileInputStream(featureFilename));
        dict = (FeatureDict) foi.readObject();

      } finally {
        if (foi != null) {
          foi.close();
        }
      }
      SgdCrf.train(fs, trainFilename, template, params.getIterationNum(), params, tc, charset,
          weights, dict);
    }

    else if (command.equals("test")) {
      if (args.length != 3 && args.length != 4) {
        showUsageAndExit();
      }
      String testFilename = args[1];
      String modelFilename = args[2];
      String charset = "UTF8";
      if (args.length > 3) {
        charset = args[3];
      }

      CrfModel model = SgdCrf.loadModel(modelFilename);
      List<Instance> instances = SgdCrf.readTestData(testFilename, charset, model.weights);
      EvaluationResult er = SgdCrf.evaluate(instances, model.weights);
      System.out.println(er);
    } else if (command.equals("test2")) {
      if (args.length != 3 && args.length != 4) {
        showUsageAndExit();
      }
      String testFilename = args[1];
      String modelFilename = args[2];
      String charset = "UTF8";
      if (args.length > 3) {
        charset = args[3];
      }

      CrfModel model = SgdCrf.loadModel(modelFilename);

      // List<Instance> instances=SgdCrf.readTestData2(testFilename,
      // charset, model.weights, new BESB1B2MTagConvertor());
      // EvaluationResult er=SgdCrf.evaluate(instances, model.weights);
      EvaluationResult er = SgdCrf.readAndEvaluate(testFilename, charset, model.weights,
          new BESB1B2MTagConvertor());
      System.out.println(er);
    } else if (command.equals("seg")) {
      if (args.length != 3 && args.length != 2 && args.length != 4) {
        showUsageAndExit();
      }
      String modelFilename = args[1];
      String charset = "";
      int nBest = 1;
      if (args.length > 2) {
        nBest = Integer.valueOf(args[2]);
      }
      if (args.length > 3) {
        charset = args[3];
      }
      CrfModel model = SgdCrf.loadModel(modelFilename);
      BufferedReader br;
      BufferedWriter bw;
      if (charset.equals("")) {
        br = new BufferedReader(new InputStreamReader(System.in));
        bw = new BufferedWriter(new OutputStreamWriter(System.out));
      } else {
        br = new BufferedReader(new InputStreamReader(System.in, charset));
        bw = new BufferedWriter(new OutputStreamWriter(System.out, charset));
      }

      String line;
      bw.write("Enter Chinese sentences to be segment, enter quit to exit!\n");
      bw.flush();
      TagConvertor tc = new BESB1B2MTagConvertor();
      while ((line = br.readLine()) != null) {
        if (line.trim().equals("quit")) {
          break;
        }
        if (line.trim().equals("")) {
          continue;
        }
        bw.write("Input: " + line + "\n");
        if (nBest < 2) {
          List<String> result = SgdCrf.segment(line, model, tc);
          boolean isFirst = true;
          for (String word : result) {
            if (isFirst) {
              isFirst = false;
            } else {
              bw.write("\t");
            }
            bw.write(word);
          }
          bw.write("\n");
          bw.write("Enter Chinese sentences to be segment, enter quit to exit!\n");
          bw.flush();
        } else {
          List<String[]> result = SgdCrf.segment(line, model, tc, nBest);

          for (String[] tks : result) {
            boolean isFirst = true;
            for (String word : tks) {
              if (isFirst) {
                isFirst = false;
              } else {
                bw.write("\t");
              }
              bw.write(word);
            }
            bw.write("\n");
          }
          bw.write("Enter Chinese sentences to be segment, enter quit to exit!\n");
          bw.flush();
        }
      }
      br.close();
      bw.close();
    } else {
      System.err.println("unknown command: " + command);
      showUsageAndExit();
    }
  }
}
