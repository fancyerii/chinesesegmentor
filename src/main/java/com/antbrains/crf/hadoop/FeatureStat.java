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

public class FeatureStat {

	public static class CounterMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		private TObjectLongHashMap<String> counter;
		private int batchSize=100000;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			counter=new TObjectLongHashMap<String>();
		}
		
		@Override 
		protected void cleanup(final Context context) throws IOException,
		InterruptedException {
			counter.forEachEntry(new TObjectLongProcedure<String>() {

				@Override
				public boolean execute(String k, long v) {
					try {
						context.write(new Text(k), new LongWritable(v));
					} catch (Exception e) {
						e.printStackTrace();
					} 
					return true;
				}
			});
				
			counter.clear();
		}
		@Override 
		public void map(Object key, Text value, final Context context)
				throws IOException, InterruptedException {
			String[] arr=value.toString().split("\t");
			if(arr.length!=2){
				return;
			}
			long c=Long.valueOf(arr[1]);
			if(c<0){
				//
				c+=Integer.MAX_VALUE;//overfloat
			}
			String f=arr[0].split(":")[0];
			String k=String.format("%s-%010d", f, c);
			
			long count=counter.get(k);
			counter.put(k, count+1);
			
			if(c<80){
				String ck="freq-"+c;
				context.getCounter(FeatureStat.class.getName(), ck).increment(1);
			}else if(c<1000){
				String ck="freq-80-1k";
				context.getCounter(FeatureStat.class.getName(), ck).increment(1);
			}else if(c<10000){
				String ck="freq-1k-10k";
				context.getCounter(FeatureStat.class.getName(), ck).increment(1);
			}else if(c<100000){
				String ck="freq-10k-100k";
				context.getCounter(FeatureStat.class.getName(), ck).increment(1);
			}else{
				String ck="freq-100k-";
				context.getCounter(FeatureStat.class.getName(), ck).increment(1);
			}
			
			if(counter.size()>=batchSize){
				counter.forEachEntry(new TObjectLongProcedure<String>() {

					@Override
					public boolean execute(String k, long v) {
						try {
							context.write(new Text(k), new LongWritable(v));
						} catch (Exception e) {
							e.printStackTrace();
						} 
						return true;
					}
				});
					
				counter.clear();
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		@Override 
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out> ");
			System.exit(2);
		}
		
		Job job = new Job(conf, FeatureStat.class.getSimpleName());

		job.setJarByClass(FeatureStat.class);
		job.setMapperClass(CounterMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
