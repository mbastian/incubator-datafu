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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import datafu.mr.avro.Schemas;
import datafu.mr.jobs.AbstractAvroJob;


/**
 * Basic Avro MR job
 *
 * @author Mathieu Bastian
 */
public class BasicAvroJob extends AbstractAvroJob {
  public static final Schema KEY_SCHEMA;
  public static final Schema VALUE_SCHEMA;
  public static final Schema OUTPUT_SCHEMA;

  static {
    KEY_SCHEMA =
        Schemas.createRecordSchema(BasicAvroJob.class, "Key", new Field("key", Schema.create(Type.LONG), "key", null));
    VALUE_SCHEMA =
        Schemas.createRecordSchema(BasicAvroJob.class, "Value", new Field("count", Schema.create(Type.LONG), "count",
            null));
    OUTPUT_SCHEMA =
        Schemas.createRecordSchema(BasicAvroJob.class, "Output",
            new Field("key", Schema.create(Type.LONG), "key", null), new Field("count", Schema.create(Type.LONG),
                "count", null));
  }

  @Override
  public Schema getMapOutputKeySchema() {
    return KEY_SCHEMA;
  }

  @Override
  public Schema getMapOutputValueSchema() {
    return VALUE_SCHEMA;
  }

  @Override
  public Schema getOutputSchema() {
    return OUTPUT_SCHEMA;
  }

  public static class TheMapper extends
      Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
    private final GenericRecord key;
    private final GenericRecord value;

    public TheMapper() {
      key = new GenericData.Record(KEY_SCHEMA);
      value = new GenericData.Record(VALUE_SCHEMA);
      value.put("count", 1L);
    }

    @Override
    protected void map(AvroKey<GenericRecord> input, NullWritable unused, Context context) throws IOException,
        InterruptedException {
      key.put("key", input.datum().get("id"));
      System.out.println("Input key=" + key.get("key"));
      context.write(new AvroKey<GenericRecord>(key), new AvroValue<GenericRecord>(value));
    }
  }

  public static class TheReducer extends
      Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
    private final GenericRecord output = new GenericData.Record(OUTPUT_SCHEMA);

    @Override
    protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context)
        throws IOException, InterruptedException {
      long count = 0L;
      for (AvroValue<GenericRecord> value : values) {
        count += (Long) value.datum().get("count");
      }
      output.put("key", key.datum().get("key"));
      output.put("count", count);
      System.out.println("Output key=" + output.get("key") + "  count=" + output.get("count"));
      context.write(new AvroKey<GenericRecord>(output), null);
    }
  }
}
