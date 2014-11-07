/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package datafu.mr.test.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import datafu.mr.jobs.AbstractJob;


/**
 * Basic map only job
 * <p>
 * The mapper inverses the input key+value.
 *
 * @author Mathieu Bastian
 */
public class BasicMapOnlyJob extends AbstractJob {

  @Override
  public void setupInputFormat(Job job) throws IOException {
    job.setInputFormatClass(SequenceFileInputFormat.class);
  }

  @Override
  public void setupOutputFormat(Job job) throws IOException {
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends Mapper> getMapperClass() {
    return MapInverse.class;
  }

  public static class MapInverse extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(value, key);
    }
  }
}
