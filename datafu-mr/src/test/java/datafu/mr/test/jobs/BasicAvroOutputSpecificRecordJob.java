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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import datafu.mr.jobs.AbstractAvroJob;
import datafu.mr.test.avro.KeyCount;


/**
 * Basic Avro MR job which outputs a specific record
 *
 * @author Mathieu Bastian
 */
public class BasicAvroOutputSpecificRecordJob extends AbstractAvroJob {

  public static class Map extends Mapper<AvroKey<GenericRecord>, NullWritable, LongWritable, LongWritable> {
    private final LongWritable key = new LongWritable();
    private final LongWritable value = new LongWritable(1L);

    @Override
    protected void map(AvroKey<GenericRecord> input, NullWritable unused, Context context) throws IOException,
        InterruptedException {
      key.set((Long) input.datum().get("id"));
      System.out.println("Input key=" + key.get());
      context.write(key, value);
    }
  }

  public static class Reduce extends Reducer<LongWritable, LongWritable, AvroKey<KeyCount>, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,
        InterruptedException {
      long count = 0L;
      for (LongWritable value : values) {
        count += (Long) value.get();
      }
      KeyCount output = new KeyCount(key.get(), count);
      context.write(new AvroKey<KeyCount>(output), null);
    }
  }
}
