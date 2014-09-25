/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.antbrains.crf.hadoop;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.Counter;

import com.antbrains.crf.BESB1B2MTagConvertor;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.Instance;
import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.TagConvertor;
import com.antbrains.crf.Template;
import com.antbrains.crf.TrainingDataSet;
import com.antbrains.crf.TrainingParams;
import com.antbrains.crf.TrainingProgress;
import com.antbrains.crf.TrainingWeights;
import com.google.gson.Gson;

public class ParallelTraining2 {

  public static class TrainingMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Gson gson = new Gson();
    private TrainingParams params;
    private TrainingWeights weights;
    private List<Instance> instances;
    private TagConvertor tc = new BESB1B2MTagConvertor();
    private int attrNum;

    public static final String FEATURE_WEIGHT = "fw";
    public static final String BOS_WEIGHT = "bw";
    public static final String EOS_WEIGHT = "ew";
    public static final String TRANSITION_WEIGHT = "tw";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      int iterate = context.getConfiguration().getInt("pt.iterate", 1);
      attrNum = context.getConfiguration().getInt("pt.featureCount", -1);
      if (iterate == 1) {
        weights = new TrainingWeights(null);

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
        // context.getCounter(TrainingMapper.class.getSimpleName(),
        // ""+(labelNum*attrNum)).increment(1);
      } else {
        // TODO read sequence file from hdfs
        weights = new TrainingWeights(null);

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

        String paramDir = context.getConfiguration().get("paramDir");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path inFile = new Path(paramDir);
        FileStatus[] status = fs.listStatus(inFile);
        double[] fw = weights.getAttributeWeights();
        double[] bos = weights.getBosTransitionWeights();
        double[] eos = weights.getEosTransitionWeights();
        double[] tw = weights.getTransitionWeights();
        Counter fwCounter = context.getCounter("ParallelTraining2", FEATURE_WEIGHT);
        Counter bosCounter = context.getCounter("ParallelTraining2", BOS_WEIGHT);
        Counter eosCounter = context.getCounter("ParallelTraining2", EOS_WEIGHT);
        Counter twCounter = context.getCounter("ParallelTraining2", TRANSITION_WEIGHT);
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
                reader = new SequenceFile.Reader(fs, f, context.getConfiguration());
                Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),
                    context.getConfiguration());
                Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),
                    context.getConfiguration());

                while (reader.next(key, value)) {
                  Text k = (Text) key;
                  DoubleWritable v = (DoubleWritable) value;
                  String s = k.toString();
                  if (s.startsWith(FEATURE_WEIGHT)) {
                    int idx = Integer.parseInt(s.substring(2));
                    fw[idx] = v.get();
                    fwCounter.increment(1);
                  } else if (s.startsWith(BOS_WEIGHT)) {
                    int idx = Integer.parseInt(s.substring(2));
                    bos[idx] = v.get();
                    bosCounter.increment(1);
                  } else if (s.startsWith(EOS_WEIGHT)) {
                    int idx = Integer.parseInt(s.substring(2));
                    eos[idx] = v.get();
                    eosCounter.increment(1);
                  } else if (s.startsWith(TRANSITION_WEIGHT)) {
                    int idx = Integer.parseInt(s.substring(2));
                    tw[idx] = v.get();
                    twCounter.increment(1);
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
      }
      // deserialize params
      String s = context.getConfiguration().get("pt.params");
      try {
        params = (TrainingParams) string2Object(s);
        params.setT0(0);// do calibrating
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      instances = new ArrayList<Instance>();
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
      doSgd(context);
    }

    private void doSgd(final Context context) throws IOException, InterruptedException {
      TrainingDataSet dataSet = new TrainingDataSet();
      dataSet.setInstances(instances);
      dataSet.setLabelNum(tc.getTagNum());
      dataSet.setAttributeNum(attrNum);
      try {
        SgdCrf.train(dataSet, 0, params.getIterationNum(), params, weights, new TrainingProgress() {

          @Override
          public void startTraining() {
            System.out.println(new java.util.Date() + " start training...");
          }

          @Override
          public void finishTraining() {
            System.out.println(new java.util.Date() + " finish training.");
          }

          @Override
          public void doValidate(String s) {
            System.out.println(new java.util.Date() + " validate result: ");
            System.out.println(s);
          }

          @Override
          public void doIter(int iter) {
            System.out.println(new java.util.Date() + " iter " + iter);
            context.progress();
          }
        });
      } catch (Exception e) {
        throw new IOException(e);
      }
      for (int i = 0; i < weights.getAttributeWeights().length; i++) {
        context.write(new Text(FEATURE_WEIGHT + i),
            new DoubleWritable(weights.getAttributeWeights()[i]));
      }
      for (int i = 0; i < weights.getBosTransitionWeights().length; i++) {
        context.write(new Text(BOS_WEIGHT + i),
            new DoubleWritable(weights.getBosTransitionWeights()[i]));
      }
      for (int i = 0; i < weights.getEosTransitionWeights().length; i++) {
        context.write(new Text(EOS_WEIGHT + i),
            new DoubleWritable(weights.getEosTransitionWeights()[i]));
      }

      for (int i = 0; i < weights.getTransitionWeights().length; i++) {
        context.write(new Text(TRANSITION_WEIGHT + i),
            new DoubleWritable(weights.getTransitionWeights()[i]));
      }
    }

    @Override
    public void map(Object key, Text value, final Context context) throws IOException,
        InterruptedException {
      String s = value.toString().split("\t", 2)[1];
      Instance instance = gson.fromJson(s, Instance.class);
      instances.add(instance);
    }
  }

  public static class TrainingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      int num = 0;
      for (DoubleWritable weights : values) {
        sum += weights.get();
        num++;
      }
      context.getCounter("TrainingReducer", "num" + num).increment(1);
      context.write(key, new DoubleWritable(sum / num));
    }

  }

  public static String object2String(Object o) throws IOException {
    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    ObjectOutputStream so = new ObjectOutputStream(bo);
    so.writeObject(o);
    so.flush();
    byte[] arr = Base64.encodeBase64(bo.toByteArray());
    return new String(arr, "UTF8");
  }

  public static Object string2Object(String s) throws IOException, ClassNotFoundException {
    byte b[] = s.getBytes("UTF8");
    byte[] bytes = Base64.decodeBase64(b);
    ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
    ObjectInputStream si = new ObjectInputStream(bi);
    return si.readObject();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    FileSystem fs = FileSystem.get(conf);
    TrainingParams params = SgdCrf.loadParams(otherArgs[3]);
    System.out.println(new Gson().toJson(params));

    if (otherArgs.length != 5) {
      System.err
          .println("ParallelTraining2 <instanceDir> <outDir> <featurecount> <training-params> <out-iter>");
      System.exit(-1);
    }
    int featureCount = Integer.valueOf(otherArgs[2]);
    // conf.set("tc", object2String(tc));
    int outIter = Integer.valueOf(otherArgs[4]);

    String prevOutDir = "";
    for (int i = 1; i <= outIter; i++) {
      System.out.println("iterator: " + i);
      conf.set("pt.iterate", i + "");
      conf.set("pt.featureCount", featureCount + "");

      conf.set("pt.params", object2String(params));
      String outDir = otherArgs[1] + "/result" + i;

      if (i > 1) {
        conf.set("paramDir", prevOutDir);
      }
      prevOutDir = outDir;
      fs.delete(new Path(outDir), true);

      Job job = new Job(conf, ParallelTraining2.class.getSimpleName());

      job.setJarByClass(ParallelTraining2.class);
      job.setMapperClass(TrainingMapper.class);
      job.setReducerClass(TrainingReducer.class);

      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      System.out.println("outDir: " + outDir);
      FileOutputFormat.setOutputPath(job, new Path(outDir));

      boolean res = job.waitForCompletion(true);
      if (!res) {
        System.err.println("iter " + i + " failed");
        break;
      }
    }
  }
}
