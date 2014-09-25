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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
import com.antbrains.crf.hadoop.ParallelTraining2.TrainingMapper;
import com.google.gson.Gson;

public class CalcFeatureWeights {

  public static class CalcFeatureMapper extends Mapper<Text, DoubleWritable, IntWritable, MyKey> {
    @Override
    public void map(Text key, DoubleWritable value, final Context context) throws IOException,
        InterruptedException {
      String s = key.toString();
      if (!s.startsWith(TrainingMapper.FEATURE_WEIGHT)) {
        return;
      }

      int idx = Integer.parseInt(s.substring(2));
      int fId = idx / 6;
      MyKey mk = new MyKey(idx % 6, value.get());
      context.write(new IntWritable(fId), mk);
    }
  }

  public static class IdentityMapper extends Mapper<MyKey, MyValue, MyKey, MyValue> {
    @Override
    public void map(MyKey key, MyValue value, final Context context) throws IOException,
        InterruptedException {
      context.write(key, value);
    }
  }

  public static class IdentityReducer extends Reducer<MyKey, MyValue, MyKey, MyValue> {
    @Override
    public void reduce(MyKey key, Iterable<MyValue> values, final Context context)
        throws IOException, InterruptedException {
      for (MyValue value : values) {
        context.write(key, value);
      }
    }
  }

  public static class CalcFeatureReducer extends Reducer<IntWritable, MyKey, MyKey, MyValue> {
    @Override
    protected void reduce(IntWritable key, Iterable<MyKey> values, Context context)
        throws IOException, InterruptedException {
      double w = 0;
      int total = 0;
      double[] array = new double[6];
      for (MyKey value : values) {
        total++;
        w += value.score * value.score;
        array[value.id] = value.score;
      }
      if (total != 6) {
        throw new IOException("not 6 for: " + key.get());
      }

      MyKey k = new MyKey(key.get(), w);
      MyValue v = new MyValue(array);
      context.write(k, v);
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 3 && otherArgs.length != 4) {
      System.err.println("CalcFeatureWeights <inDir> <tmpDir> <outDir> [startStep]");
      System.exit(-1);
    }
    int startStep = 1;
    if (otherArgs.length == 4) {
      startStep = Integer.valueOf(otherArgs[otherArgs.length - 1]);
    }
    FileSystem fs = FileSystem.get(conf);
    if (startStep <= 1) {
      System.out.println("calc");
      fs.delete(new Path(otherArgs[1]), true);
      Job job = new Job(conf, CalcFeatureWeights.class.getSimpleName());
      job.setNumReduceTasks(1);
      job.setJarByClass(CalcFeatureWeights.class);
      job.setMapperClass(CalcFeatureMapper.class);
      job.setReducerClass(CalcFeatureReducer.class);

      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setInputFormatClass(SequenceFileInputFormat.class);

      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(MyKey.class);

      job.setOutputKeyClass(MyKey.class);
      job.setOutputValueClass(MyValue.class);
      FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));

      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

      boolean res = job.waitForCompletion(true);
      if (!res) {
        System.err.println("step1 failed");
        return;
      }
    }

    if (startStep <= 2)
    // sort
    {
      fs.delete(new Path(otherArgs[2]), true);
      System.out.println("sort");
      Job job = new Job(conf, CalcFeatureWeights.class.getSimpleName());

      job.setNumReduceTasks(1);
      job.setJarByClass(CalcFeatureWeights.class);
      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(IdentityReducer.class);

      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setInputFormatClass(SequenceFileInputFormat.class);

      job.setMapOutputKeyClass(MyKey.class);
      job.setMapOutputValueClass(MyValue.class);
      job.setOutputKeyClass(MyKey.class);
      job.setOutputValueClass(MyValue.class);

      FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));

      FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

      boolean res = job.waitForCompletion(true);
      if (!res) {
        System.err.println("step2 failed");
        return;
      }
    }

  }
}
