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

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.antbrains.crf.BESB1B2MTagConvertor;
import com.antbrains.crf.FeatureDict;
import com.antbrains.crf.Instance;
import com.antbrains.crf.SgdCrf;
import com.antbrains.crf.TagConvertor;
import com.antbrains.crf.Template;
import com.google.gson.Gson;

import de.ruedigermoeller.serialization.FSTObjectInput;

public class InstanceGenerator {

	public static class CounterMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		private FeatureDict fd = null;
		private Template template;
		private TagConvertor tc = new BESB1B2MTagConvertor();
		private TObjectIntHashMap<String> labelDict;
		private int reducerNum = 100;
		private long lineNumber = 0;
		private Gson gson=new Gson();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FSTObjectInput foi = null;
			String dict = context.getConfiguration().get("dict");
			if (dict == null)
				throw new IOException("dict is null");
			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			if (paths.length != 1)
				throw new IOException("paths.length=" + paths.length);
			try {
				foi = new FSTObjectInput(new FileInputStream(
						paths[0].toString()));
				try {
					fd = (FeatureDict) foi.readObject();
					int size=fd.size();
					context.getCounter(InstanceGenerator.class.getSimpleName(), ""+size).increment(1);
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				}

			} finally {
				if (foi != null) {
					foi.close();
				}
			}
			labelDict = new TObjectIntHashMap<String>();
			for (String tag : tc.getTags()) {
				labelDict.put(tag, labelDict.size());
			}
			try {
				template = (Template) string2Object(context.getConfiguration()
						.get("template"));
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
//			reducerNum = context.getConfiguration().getInt("mapred.map.tasks",
//					reducerNum);
		}

		@Override
		protected void cleanup(final Context context) throws IOException,
				InterruptedException {
			// lineNumber=0;
		}

		@Override
		public void map(Object key, Text value, final Context context)
				throws IOException, InterruptedException {
			String[] tks = value.toString().split("\t");
			Instance instance = SgdCrf.buildInstance(tks, tc, fd, labelDict,
					template, false, false);
			int k = (int) ((lineNumber++) % this.reducerNum);
			context.write(new IntWritable(k), new Text(gson.toJson(instance)));
		}
	}

	public static String object2String(Object o) throws IOException {
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream so = new ObjectOutputStream(bo);
		so.writeObject(o);
		so.flush();
		byte[] arr=Base64.encodeBase64(bo.toByteArray());
		return new String(arr,"UTF8");
	}

	public static Object string2Object(String s) throws IOException,
			ClassNotFoundException {
		byte b[] = s.getBytes("UTF8");
		byte[] bytes=Base64.decodeBase64(b);
		ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
		ObjectInputStream si = new ObjectInputStream(bi);
		return si.readObject();
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("InstanceGenerator <in> <out> <featuredict> <template>");
			System.exit(-1);
		}
		Template template = new Template(otherArgs[3], "UTF8");
		conf.set("template", object2String(template));
		// conf.set("tc", object2String(tc));

		DistributedCache.addCacheFile(new URI(otherArgs[2]), conf);
		conf.set("dict", otherArgs[2]);
		conf.set("mapred.reduce.tasks", "0");
		Job job = new Job(conf, InstanceGenerator.class.getSimpleName());

		job.setJarByClass(InstanceGenerator.class);
		job.setMapperClass(CounterMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
