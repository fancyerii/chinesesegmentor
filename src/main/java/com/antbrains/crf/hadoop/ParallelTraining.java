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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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

public class ParallelTraining {

  public static class TrainingMapper extends Mapper<Object, Text, NullWritable, TrainingWeights> {

    private Gson gson = new Gson();
    private TrainingParams params;
    private TrainingWeights weights;
    private List<Instance> instances;
    private TagConvertor tc = new BESB1B2MTagConvertor();
    private int attrNum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      int iterate = context.getConfiguration().getInt("pt.iterate", 1);
      if (iterate == 1) {
        attrNum = context.getConfiguration().getInt("pt.featureCount", -1);
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
        // TODO read from hdfs
      }
      // deserialize params
      String s = context.getConfiguration().get("pt.params");
      try {
        params = (TrainingParams) string2Object(s);
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
      context.write(NullWritable.get(), weights);
    }

    @Override
    public void map(Object key, Text value, final Context context) throws IOException,
        InterruptedException {
      String s = value.toString().split("\t", 2)[1];
      Instance instance = gson.fromJson(s, Instance.class);
      instances.add(instance);
    }
  }

  public static class TrainingReducer extends
      Reducer<NullWritable, TrainingWeights, NullWritable, TrainingWeights> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(NullWritable key, Iterable<TrainingWeights> values, Context context)
        throws IOException, InterruptedException {
      TrainingWeights result = null;
      int total = 0;
      for (TrainingWeights weights : values) {
        if (result == null) {
          result = weights;
        } else {
          addWeights(result, weights);
        }
        total++;
      }
      if (total > 1) {
        divideWeights(result, total);
      }
      context.write(NullWritable.get(), result);
    }

    private void addWeights(TrainingWeights w1, TrainingWeights w2) {
      addArray(w1.getAttributeWeights(), w2.getAttributeWeights());
      addArray(w1.getBosTransitionWeights(), w2.getBosTransitionWeights());
      addArray(w1.getEosTransitionWeights(), w2.getEosTransitionWeights());
      addArray(w1.getTransitionWeights(), w2.getTransitionWeights());
    }

    private void addArray(double[] arr1, double[] arr2) {
      for (int i = 0; i < arr1.length; i++) {
        arr1[i] += arr2[i];
      }
    }

    private void divideWeights(TrainingWeights w, int total) {
      divArray(w.getAttributeWeights(), total);
      divArray(w.getBosTransitionWeights(), total);
      divArray(w.getEosTransitionWeights(), total);
      divArray(w.getTransitionWeights(), total);
    }

    private void divArray(double[] arr, int total) {
      for (int i = 0; i < arr.length; i++) {
        arr[i] /= total;
      }
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
    if (otherArgs.length != 4) {
      System.err
          .println("ParallelTraining <instanceDir> <outDir> <featurecount> <training-params>");
      System.exit(-1);
    }
    int featureCount = Integer.valueOf(otherArgs[2]);
    // conf.set("tc", object2String(tc));

    conf.set("pt.iterate", "1");
    conf.set("pt.featureCount", featureCount + "");

    TrainingParams params = SgdCrf.loadParams(otherArgs[3]);
    System.out.println(new Gson().toJson(params));
    conf.set("pt.params", object2String(params));

    Job job = new Job(conf, ParallelTraining.class.getSimpleName());

    job.setJarByClass(ParallelTraining.class);
    job.setMapperClass(TrainingMapper.class);
    job.setReducerClass(TrainingReducer.class);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(TrainingWeights.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
