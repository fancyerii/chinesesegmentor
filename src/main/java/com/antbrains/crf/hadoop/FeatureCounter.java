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
import gnu.trove.procedure.TObjectIntProcedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.Template;

public class FeatureCounter {

  public static class CounterMapper extends Mapper<Object, Text, Text, LongWritable> {
    private Template template;

    private TObjectIntHashMap<String> counter;
    private int batchSize = 100000;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String s = conf.get("templates");
      if (s == null)
        throw new IllegalArgumentException("templates is null");
      template = new Template(str2StrArr(s));
      counter = new TObjectIntHashMap<String>((int) (this.batchSize * 1.25), 0.8f, 0);
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
      counter.forEachEntry(new TObjectIntProcedure<String>() {
        @Override
        public boolean execute(String text, int index) {
          try {
            context.write(new Text(text), new LongWritable(index));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return true;
        }
      });
      counter.clear();
    }

    @Override
    public void map(Object key, Text value, final Context context) throws IOException,
        InterruptedException {
      String sen = value.toString().replaceAll("\t", "");
      sen = StringTools.normalizeQuery(sen);
      List<String> attributes = new ArrayList<String>(sen.length());
      for (int i = 0; i < sen.length(); i++) {
        attributes.add(sen.charAt(i) + "");
      }
      List<String> features = template.expandTemplate(attributes, attributes.size());
      for (String feature : features) {
        if (feature.contains("_B-") || feature.contains("_B+")) {
          continue;
        }
        int count = counter.get(feature);
        counter.put(feature, count + 1);
      }

      if (counter.size() >= batchSize) {
        counter.forEachEntry(new TObjectIntProcedure<String>() {
          @Override
          public boolean execute(String text, int index) {
            try {
              context.write(new Text(text), new LongWritable(index));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return true;
          }
        });
        counter.clear();
      }
    }
  }

  public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  private static String[] str2StrArr(String s) {
    return s.split("\n");
  }

  private static String strArr2Str(String[] arr) {
    StringBuilder sb = new StringBuilder("");
    for (String s : arr) {
      s = s.replaceAll("\n", "");
      sb.append(s).append("\n");
    }
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: wordcount <in> <out> <templatefile>");
      System.exit(2);
    }

    String[] templates = SgdCrf.readTemplates(otherArgs[2]).toArray(new String[0]);
    conf.set("templates", strArr2Str(templates));

    Job job = new Job(conf, FeatureCounter.class.getSimpleName());

    job.setJarByClass(FeatureCounter.class);
    job.setMapperClass(CounterMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
