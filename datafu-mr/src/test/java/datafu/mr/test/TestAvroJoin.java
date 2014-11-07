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

package datafu.mr.test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.avro.Schemas;
import datafu.mr.test.jobs.AvroJoin;
import datafu.mr.test.util.BasicAvroReader;
import datafu.mr.test.util.BasicAvroWriter;


@Test(groups = "pcl")
public class TestAvroJoin extends TestBase {

  private static Schema SCHEMA_A = Schemas.createRecordSchema(TestAvroJoin.class, "KeyA",
      new Field("key", Schema.create(Type.STRING), "key", null), new Field("value_a", Schema.create(Type.STRING),
          "value_a", null));

  private static Schema SCHEMA_B = Schemas.createRecordSchema(TestAvroJoin.class, "KeyB",
      new Field("key", Schema.create(Type.STRING), "key", null), new Field("value_b", Schema.create(Type.STRING),
          "value_b", null));

  private final Logger _log = Logger.getLogger(TestAbstractJob.class);
  private final Path _inputPathA = new Path("/inputA");
  private final Path _inputPathB = new Path("/inputB");
  private final Path _outputPath = new Path("/output");

  @BeforeMethod
  public void beforeMethod(Method method) throws IOException {
    _log.info("*** Running " + method.getName());

    _log.info("*** Cleaning input and output paths");
    getFileSystem().delete(_inputPathA, true);
    getFileSystem().mkdirs(_inputPathA);
    getFileSystem().delete(_inputPathB, true);
    getFileSystem().mkdirs(_inputPathB);
    getFileSystem().delete(_outputPath, true);
  }

  @Test
  public void testInnerJoin() throws IOException, InterruptedException, ClassNotFoundException {
    initInputA();
    initInputB();
    configureAndRunJob(new AvroJoin(), "AvroJoin", new Path(_inputPathA.toString() + "," + _inputPathB.toString()),
        _outputPath, "key", "inner");
    checkInnerJoinOutput();
  }

  @Test
  public void testOuterJoin() throws IOException, InterruptedException, ClassNotFoundException {
    initInputA();
    initInputB();
    configureAndRunJob(new AvroJoin(), "AvroJoin", new Path(_inputPathA.toString() + "," + _inputPathB.toString()),
        _outputPath, "key", "outer");
    checkOuterJoinOutput();
  }

  // UTILITIES

  private void initInputA() throws IOException {
    BasicAvroWriter<GenericRecord> writer = new BasicAvroWriter<GenericRecord>(_inputPathA, SCHEMA_A, getFileSystem());
    writer.open();
    GenericRecord record1 = new GenericData.Record(SCHEMA_A);
    record1.put("key", "mykey");
    record1.put("value_a", "foo");
    writer.append(record1);
    GenericRecord record2 = new GenericData.Record(SCHEMA_A);
    record2.put("key", "mykey2");
    record2.put("value_a", "foo");
    writer.append(record2);
    writer.close();
  }

  private void initInputB() throws IOException {
    BasicAvroWriter<GenericRecord> writer = new BasicAvroWriter<GenericRecord>(_inputPathB, SCHEMA_B, getFileSystem());
    GenericRecord recordA = new GenericData.Record(SCHEMA_B);
    recordA.put("key", "mykey");
    recordA.put("value_b", "bar");
    writer.writeAll(recordA);
  }

  private void checkInnerJoinOutput() throws IOException {
    BasicAvroReader<GenericRecord> reader = new BasicAvroReader<GenericRecord>(_outputPath, getFileSystem());
    List<GenericRecord> results = reader.readAll();
    Assert.assertEquals(results.size(), 1);
    GenericRecord record = results.get(0);
    Assert.assertEquals(record.get("key").toString(), "mykey");
    Assert.assertEquals(record.get("value_a").toString(), "foo");
    Assert.assertEquals(record.get("value_b").toString(), "bar");
  }

  private void checkOuterJoinOutput() throws IOException {
    BasicAvroReader<GenericRecord> reader = new BasicAvroReader<GenericRecord>(_outputPath, getFileSystem());
    List<GenericRecord> results = reader.readAll();
    Assert.assertEquals(results.size(), 2);
    GenericRecord record = results.get(0);
    Assert.assertEquals(record.get("key").toString(), "mykey");
    Assert.assertEquals(record.get("value_a").toString(), "foo");
    Assert.assertEquals(record.get("value_b").toString(), "bar");
    GenericRecord record2 = results.get(1);
    Assert.assertEquals(record2.get("key").toString(), "mykey2");
    Assert.assertEquals(record2.get("value_a").toString(), "foo");
    Assert.assertNull(record2.get("value_b"));
  }

  private void configureAndRunJob(AvroJoin job, String name, Path inputPath, Path outputPath, String joinKeys,
      String type) throws IOException, ClassNotFoundException, InterruptedException {
    Properties _props = newTestProperties();
    _props.setProperty("input.path", inputPath.toString());
    _props.setProperty("output.path", outputPath.toString());
    _props.setProperty("avrojoin.keys", joinKeys);
    _props.setProperty("avrojoin.type", type);
    job.setProperties(_props);
    job.setName(name);
    job.run();
  }
}
