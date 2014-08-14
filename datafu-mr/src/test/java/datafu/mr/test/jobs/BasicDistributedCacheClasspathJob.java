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
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import datafu.mr.jobs.AbstractJob;


/**
 * Basic MR job which uses the distributed cache for loading JARs into the classpath.
 *
 * @author Mathieu Bastian
 */
public class BasicDistributedCacheClasspathJob extends AbstractJob {

  private final boolean folderOnly;

  public BasicDistributedCacheClasspathJob(boolean folderOnly) {
    this.folderOnly = folderOnly;
  }

  @Override
  public void setupInputFormat(Job job) throws IOException {
    job.setInputFormatClass(SequenceFileInputFormat.class);
  }

  @Override
  public void setupOutputFormat(Job job) throws IOException {
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
  }

  @Override
  public List<Path> getDistributedCacheClasspaths() {
    if (folderOnly) {
      return Arrays.asList(new Path[] { new Path("/cache") });
    }
    return Arrays.asList(new Path[] { new Path("/cache/localjar.jar"), new Path("/cache/localjar2.jar") });
  }

  public static class Map extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      testFilePresence("foofile");
      testFilePresence("barfile");
    }

    private void testFilePresence(String fileName) throws IOException {
      InputStream stream = getClass().getResourceAsStream("/" + fileName);

      if (stream == null) {
        throw new RuntimeException(String.format("The distributed classpath file '%s' doesn't exist", fileName));
      }

      int b = stream.read();
      if (b != 42) {
        throw new RuntimeException(String.format("The %s content isn't correct", fileName));
      }
      stream.close();
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
    }
  }
}
