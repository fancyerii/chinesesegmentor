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

import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.procedure.TLongLongProcedure;
import gnu.trove.procedure.TObjectIntProcedure;
import gnu.trove.procedure.TObjectLongProcedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class FeatureFilter {

  public static class CounterMapper extends Mapper<Object, Text, Text, LongWritable> {
    Map<String, Integer> filterMap = new HashMap<String, Integer>();
    private boolean statOnly = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      String rules = context.getConfiguration().get("rules");
      if (rules == null) {
        throw new IllegalArgumentException("need rules");
      }
      String[] lines = rules.split("\n");
      for (String line : lines) {
        String[] arr = line.split("\\s+");
        if (arr.length == 2) {
          filterMap.put(arr[0], Integer.valueOf(arr[1]));
        }
      }
      String s = context.getConfiguration().get("statOnly");
      if (s.equalsIgnoreCase("false")) {
        statOnly = false;
      }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {

    }

    @Override
    public void map(Object key, Text value, final Context context) throws IOException,
        InterruptedException {
      String[] arr = value.toString().split("\t");
      if (arr.length != 2) {
        return;
      }
      long c = Long.valueOf(arr[1]);
      if (c < 0) {
        context.getCounter(FeatureFilter.class.getSimpleName(), "old- " + c).increment(1);
        //
        c = (long) ((int) c - Integer.MAX_VALUE) + Integer.MAX_VALUE;
        context.getCounter(FeatureFilter.class.getSimpleName(), "new- " + c).increment(1);
      }
      String f = arr[0].split(":")[0];
      Integer threshold = filterMap.get(f);
      if (threshold != null && c <= threshold) {
        context.getCounter(FeatureFilter.class.getSimpleName(), "filter-" + f).increment(1);
      } else {
        if (!statOnly) {
          context.write(new Text(arr[0]), new LongWritable(c));
        }
        context.getCounter(FeatureFilter.class.getSimpleName(), "keep-" + f).increment(1);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: wordcount <in> <out> filterRuleFile statOnly");
      System.exit(-1);
    }

    boolean statOnly = true;
    if (otherArgs[3].equalsIgnoreCase("false")) {
      statOnly = false;
    }
    conf.set("statOnly", statOnly + "");

    String rules = FileTools.readFile(otherArgs[2], "UTF8");
    conf.set("rules", rules);
    conf.set("mapred.reduce.tasks", "0");
    Job job = new Job(conf, FeatureFilter.class.getSimpleName());

    job.setJarByClass(FeatureFilter.class);
    job.setMapperClass(CounterMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
