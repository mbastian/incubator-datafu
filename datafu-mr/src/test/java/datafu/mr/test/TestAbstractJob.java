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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.jobs.AbstractJob;
import datafu.mr.test.jobs.BasicAvroWordCountOverrideFormatJob;
import datafu.mr.test.jobs.BasicConcatMultipleInputsJob;
import datafu.mr.test.jobs.BasicDistributedCacheClasspathJob;
import datafu.mr.test.jobs.BasicDistributedCacheJob;
import datafu.mr.test.jobs.BasicMapOnlyJob;
import datafu.mr.test.jobs.BasicMultipleOutputsJob;
import datafu.mr.test.jobs.BasicWordCountJob;
import datafu.mr.test.util.BasicWritableReader;
import datafu.mr.test.util.BasicWritableWriter;


@Test(groups = "pcl")
public class TestAbstractJob extends TestBase {

  private final Logger _log = Logger.getLogger(TestAbstractJob.class);
  private final Path _inputPath = new Path("/input");
  private final Path _cachePath = new Path("/cache");
  private final Path _outputPath = new Path("/output");

  @BeforeMethod
  public void beforeMethod(Method method) throws IOException {
    _log.info("*** Running " + method.getName());

    _log.info("*** Cleaning input and output paths");
    getFileSystem().delete(_inputPath, true);
    getFileSystem().mkdirs(_inputPath);
    getFileSystem().delete(_cachePath, true);
  }

  @AfterMethod
  public void afterMethod(Method method) throws IOException {
    getFileSystem().delete(_inputPath, true);
    getFileSystem().delete(_cachePath, true);
    getFileSystem().delete(_outputPath, true);
  }

  @Test
  public void testBasicWordCountJob() throws IOException, InterruptedException, ClassNotFoundException {
    writeWordCountInput();
    configureAndRunJob(new BasicWordCountJob(), "BasicWordCountJob", _inputPath, _outputPath);
    checkWordCountOutput();
  }

  @Test
  public void testBasicMapOnlyJob() throws IOException, InterruptedException, ClassNotFoundException {
    writeMapOnlyInput();
    configureAndRunJob(new BasicMapOnlyJob(), "BasicMapOnlyJob", _inputPath, _outputPath);
    checkMapOnlyOutput();
  }

  @Test
  public void testConcatMultipleInputsJob() throws IOException, InterruptedException, ClassNotFoundException {
    writeConcatInput(new Path(_inputPath, "A"), "foo");
    writeConcatInput(new Path(_inputPath, "B"), "bar");
    Path multipleInput = new Path(_inputPath.toString() + "/A," + _inputPath.toString() + "/B");
    configureAndRunJob(new BasicConcatMultipleInputsJob(), "BasicConcatMultipleInputsJob", multipleInput, _outputPath);
    checkConcatOutput("foo", "bar");
  }

  @Test
  public void testDistributedCacheJob() throws IOException, InterruptedException, ClassNotFoundException {
    writeCacheFile();
    configureAndRunJob(new BasicDistributedCacheJob(true), "BasicDistributedCacheJob", _inputPath, _outputPath);
  }

  @Test
  public void testDistributedCacheJobWithoutSymlinkJob() throws IOException, InterruptedException,
      ClassNotFoundException {
    writeCacheFile();
    configureAndRunJob(new BasicDistributedCacheJob(false), "BasicDistributedCacheJob", _inputPath, _outputPath);
  }

  @Test
  public void testDistributedCacheClasspathJob() throws IOException, InterruptedException, ClassNotFoundException {
    writeCacheJars();
    configureAndRunJob(new BasicDistributedCacheClasspathJob(false), "BasicDistributedCacheClasspathJob", _inputPath,
        _outputPath);
  }

  @Test
  public void testDistributedCacheClasspathJobFolderOnly() throws IOException, InterruptedException,
      ClassNotFoundException {
    writeCacheJars();
    configureAndRunJob(new BasicDistributedCacheClasspathJob(true), "BasicDistributedCacheClasspathJob", _inputPath,
        _outputPath);
  }

  @Test
  public void testMultipleOutputsJob() throws IOException, InterruptedException, ClassNotFoundException {
    writeMultipleOutputsInput();
    configureAndRunJob(new BasicMultipleOutputsJob(), "BasicMultipleOutputsJob", _inputPath, _outputPath);
    checkMultipleOutputs();
  }

  @Test
  public void testBasicAvroWordCountOverrideFormatJob() throws IOException, InterruptedException,
      ClassNotFoundException {
    writeWordCountInput();
    configureAndRunJob(new BasicAvroWordCountOverrideFormatJob(), "BasicAvroWordCountOverrideFormatJob", _inputPath,
        _outputPath);
    checkWordCountOutput();
  }

  // UTILITIES

  private void writeWordCountInput() throws IOException {
    BasicWritableWriter<LongWritable, Text> writer =
        new BasicWritableWriter<LongWritable, Text>(_inputPath, getFileSystem(), LongWritable.class, Text.class);
    writer.open();
    writer.append(new LongWritable(0L), new Text("hello world"));
    writer.append(new LongWritable(1L), new Text("hello"));
    writer.close();
  }

  private void checkWordCountOutput() throws IOException {
    BasicWritableReader<Text, IntWritable> reader =
        new BasicWritableReader<Text, IntWritable>(_outputPath, getFileSystem(), Text.class, IntWritable.class);
    reader.open();
    Map<Text, IntWritable> res = reader.readAll();
    reader.close();

    Assert.assertNotNull(res);
    Assert.assertEquals(res.size(), 2);
    Assert.assertEquals(res.get(new Text("hello")).get(), 2);
    Assert.assertEquals(res.get(new Text("world")).get(), 1);
  }

  private void writeMapOnlyInput() throws IOException {
    BasicWritableWriter<LongWritable, Text> writer =
        new BasicWritableWriter<LongWritable, Text>(_inputPath, getFileSystem(), LongWritable.class, Text.class);
    writer.open();
    writer.append(new LongWritable(0L), new Text("foo"));
    writer.append(new LongWritable(1L), new Text("bar"));
    writer.close();
  }

  private void checkMapOnlyOutput() throws IOException {
    BasicWritableReader<Text, LongWritable> reader =
        new BasicWritableReader<Text, LongWritable>(_outputPath, getFileSystem(), Text.class, LongWritable.class);
    reader.open();
    Map<Text, LongWritable> res = reader.readAll();
    reader.close();

    Assert.assertNotNull(res);
    Assert.assertEquals(res.size(), 2);
    Assert.assertEquals(res.get(new Text("foo")).get(), 0L);
    Assert.assertEquals(res.get(new Text("bar")).get(), 1L);
  }

  private void writeConcatInput(Path path, String value) throws IOException {
    BasicWritableWriter<IntWritable, Text> writer =
        new BasicWritableWriter<IntWritable, Text>(path, getFileSystem(), IntWritable.class, Text.class);
    writer.open();
    writer.append(new IntWritable(0), new Text(value));
    writer.close();
  }

  private void checkConcatOutput(String value1, String value2) throws IOException {
    BasicWritableReader<IntWritable, Text> reader =
        new BasicWritableReader<IntWritable, Text>(_outputPath, getFileSystem(), IntWritable.class, Text.class);
    reader.open();
    Map<IntWritable, Text> res = reader.readAll();
    reader.close();

    Assert.assertNotNull(res);
    Assert.assertEquals(res.size(), 1);
    Text val = res.get(new IntWritable(0));
    Assert.assertTrue(new Text(value1 + value2).equals(val) || (new Text(value2 + value1).equals(val)));
  }

  private void writeCacheFile() throws IOException {
    FSDataOutputStream fin = getFileSystem().create(_cachePath);
    fin.writeUTF("hello");
    fin.close();

    BasicWritableWriter<IntWritable, IntWritable> writer =
        new BasicWritableWriter<IntWritable, IntWritable>(_inputPath, getFileSystem(), IntWritable.class,
            IntWritable.class);
    writer.open();
    writer.append(new IntWritable(0), new IntWritable(1));
    writer.close();
  }

  private void writeCacheJars() throws IOException {
    writeCacheJar("localjar.jar", "foofile");
    writeCacheJar("localjar2.jar", "barfile");

    BasicWritableWriter<IntWritable, IntWritable> writer =
        new BasicWritableWriter<IntWritable, IntWritable>(_inputPath, getFileSystem(), IntWritable.class,
            IntWritable.class);
    writer.open();
    writer.append(new IntWritable(0), new IntWritable(1));
    writer.close();
  }

  private void writeCacheJar(String jarName, String fileName) throws IOException {
    File localJar = new File(jarName);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream target = new JarOutputStream(new FileOutputStream(localJar), manifest);
    JarEntry entry = new JarEntry(fileName);
    entry.setTime(localJar.lastModified());
    target.putNextEntry(entry);
    target.write(42);
    target.closeEntry();
    target.close();

    getFileSystem().copyFromLocalFile(true, new Path(localJar.getAbsolutePath()),
        new Path(_cachePath, localJar.getName()));
  }

  private void writeMultipleOutputsInput() throws IOException {
    BasicWritableWriter<IntWritable, Text> writer =
        new BasicWritableWriter<IntWritable, Text>(_inputPath, getFileSystem(), IntWritable.class, Text.class);
    writer.open();
    writer.append(new IntWritable(0), new Text("foo"));
    writer.append(new IntWritable(1), new Text("bar"));
    writer.close();
  }

  private void checkMultipleOutputs() throws IOException {
    BasicWritableReader<IntWritable, Text> reader1 =
        new BasicWritableReader<IntWritable, Text>(new Path(_outputPath + "/foo"), getFileSystem(), IntWritable.class,
            Text.class);
    reader1.open();
    Map<IntWritable, Text> res1 = reader1.readAll();
    reader1.close();

    Assert.assertNotNull(res1);
    Assert.assertEquals(res1.size(), 1);
    Assert.assertEquals(res1.get(new IntWritable(0)), new Text("foo"));

    BasicWritableReader<IntWritable, Text> reader2 =
        new BasicWritableReader<IntWritable, Text>(new Path(_outputPath + "/bar"), getFileSystem(), IntWritable.class,
            Text.class);
    reader2.open();
    Map<IntWritable, Text> res2 = reader2.readAll();
    reader2.close();

    Assert.assertNotNull(res2);
    Assert.assertEquals(res2.size(), 1);
    Assert.assertEquals(res2.get(new IntWritable(1)), new Text("bar"));
  }

  private void configureAndRunJob(AbstractJob job, String name, Path inputPath, Path outputPath) throws IOException,
      ClassNotFoundException, InterruptedException {
    Properties _props = newTestProperties();
    _props.setProperty("input.path", inputPath.toString());
    _props.setProperty("output.path", outputPath.toString());
    job.setProperties(_props);
    job.setName(name);
    job.run();
  }
}
