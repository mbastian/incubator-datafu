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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.avro.Schemas;
import datafu.mr.fs.DatePath;
import datafu.mr.fs.PathUtils;
import datafu.mr.test.util.BasicAvroWriter;


@Test(groups = "pcl")
public class TestPathUtils extends TestBase {

  private final Logger _log = Logger.getLogger(TestPathUtils.class);
  private final Path _inputPath = new Path("/input");
  private final Path _outputPath = new Path("/output");

  @BeforeMethod
  public void beforeMethod(Method method) throws IOException {
    _log.info("*** Running " + method.getName());

    _log.info("*** Cleaning input and output paths");
    getFileSystem().delete(_inputPath, true);
    getFileSystem().delete(_outputPath, true);
    getFileSystem().mkdirs(_inputPath);
    getFileSystem().mkdirs(_outputPath);
  }

  @Test
  public void keepLatestDatedPathsTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Path outputA = new Path(_inputPath, "2014-01-01");
    writeFile(outputA, "");
    Path outputB = new Path(_inputPath, "2014-01-03");
    writeFile(outputB, "");
    Path outputC = new Path(_inputPath, "2014-01-02");
    writeFile(outputC, "");

    PathUtils.keepLatestDatedPaths(getFileSystem(), _inputPath, 1, sdf);

    FileStatus[] fss = getFileSystem().listStatus(_inputPath);
    Assert.assertEquals(fss.length, 1);
    Assert.assertEquals(fss[0].getPath().toUri().getPath().toString(), outputB.toString());
  }

  @Test
  public void keepLatestDatedPathsWithOtherFilesTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Path outputA = new Path(_inputPath, "2014-01-01");
    writeFile(outputA, "");
    Path outputB = new Path(_inputPath, "foo");
    writeFile(outputB, "");

    PathUtils.keepLatestDatedPaths(getFileSystem(), _inputPath, 1, sdf);

    FileStatus[] fss = getFileSystem().listStatus(_inputPath);
    Assert.assertEquals(fss.length, 2);
    Assert.assertEquals(fss[0].getPath().toUri().getPath().toString(), outputA.toString());
    Assert.assertEquals(fss[1].getPath().toUri().getPath().toString(), outputB.toString());
  }

  @Test
  public void keepLatestDatedPathsEmptyTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    PathUtils.keepLatestDatedPaths(getFileSystem(), _inputPath, 1, sdf);
    Assert.assertTrue(getFileSystem().exists(_inputPath));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void keepLatestDatedPathsBadNumberTest() throws IOException {
    PathUtils.keepLatestDatedPaths(getFileSystem(), _inputPath, 0, new SimpleDateFormat("yyyy-MM-dd"));
  }

  @Test(expectedExceptions = IOException.class)
  public void keepLatestDatedPathsMisisngPathTest() throws IOException {
    PathUtils.keepLatestDatedPaths(getFileSystem(), new Path("foo"), 1, new SimpleDateFormat("yyyy-MM-dd"));
  }

  @Test
  public void keepLatestNestedDatedPathsTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

    Path outputA = new Path(_inputPath, "2014/01/01");
    writeFile(outputA, "");
    Path outputB = new Path(_inputPath, "2014/01/03");
    writeFile(outputB, "");
    Path outputC = new Path(_inputPath, "2014/01/02");
    writeFile(outputC, "");

    PathUtils.keepLatestNestedDatedPaths(getFileSystem(), _inputPath, 1, sdf);

    FileStatus[] fss = getFileSystem().listStatus(_inputPath);
    Assert.assertEquals(fss.length, 1);
    Assert.assertTrue(getFileSystem().exists(outputB));
    Assert.assertFalse(getFileSystem().exists(outputC));
    Assert.assertFalse(getFileSystem().exists(outputA));
  }

  @Test
  public void findDatedPathsTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    Path outputA = new Path(_inputPath, "2014-01-01");
    writeFile(outputA, "");
    Path outputB = new Path(_inputPath, "2014-01-03");
    writeFile(outputB, "");

    List<DatePath> datePaths = PathUtils.findDatedPaths(getFileSystem(), _inputPath, sdf);
    Assert.assertEquals(datePaths.size(), 2);
    Assert.assertEquals(datePaths.get(0).getPath().toUri().getPath(), outputA.toString());
    Assert.assertEquals(datePaths.get(1).getPath().toUri().getPath(), outputB.toString());
  }

  @Test(expectedExceptions = IOException.class)
  public void findDatedPathsMissingPathTest() throws IOException {
    PathUtils.findDatedPaths(getFileSystem(), new Path("foo"), new SimpleDateFormat("yyyy-MM-dd"));
  }

  @Test
  public void getSchemaFromFileTest() throws IOException {
    Schema schema =
        Schemas.createRecordSchema(TestPathUtils.class, "Event", new Field("id", Schema.create(Type.INT), "ID", null));

    BasicAvroWriter<GenericRecord> writer = new BasicAvroWriter<GenericRecord>(_inputPath, schema, getFileSystem());
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 42);
    writer.writeAll(record);

    Schema schemaFound = PathUtils.getSchemaFromFile(getFileSystem(), writer.getOutputFilePath());
    Assert.assertEquals(schemaFound, schema);
  }

  @Test
  public void getSchemaFromPathTest() throws IOException {
    Schema schema =
        Schemas.createRecordSchema(TestPathUtils.class, "Event", new Field("id", Schema.create(Type.INT), "ID", null));

    BasicAvroWriter<GenericRecord> writer = new BasicAvroWriter<GenericRecord>(_inputPath, schema, getFileSystem());
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 42);
    writer.writeAll(record);

    Schema schemaFound = PathUtils.getSchemaFromPath(getFileSystem(), _inputPath);
    Assert.assertEquals(schemaFound, schema);
  }

  @Test
  public void getSchemaFromPathMixedNonAvroTest() throws IOException {
    Path outputA = new Path(_inputPath, "foo");
    writeFile(outputA, "bar");

    Schema schema =
        Schemas.createRecordSchema(TestPathUtils.class, "Event", new Field("id", Schema.create(Type.INT), "ID", null));

    BasicAvroWriter<GenericRecord> writer = new BasicAvroWriter<GenericRecord>(_inputPath, schema, getFileSystem());
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 42);
    writer.writeAll(record);

    Schema schemaFound = PathUtils.getSchemaFromPath(getFileSystem(), _inputPath);
    Assert.assertEquals(schemaFound, schema);
  }

  @Test(expectedExceptions = IOException.class)
  public void getSchemaFromPathNonAvroTest() throws IOException {
    Path outputA = new Path(_inputPath, "foo");
    writeFile(outputA, "bar");

    PathUtils.getSchemaFromPath(getFileSystem(), outputA);
  }

  @Test(expectedExceptions = IOException.class)
  public void getSchemaFromFileNonAvroTest() throws IOException {
    Path outputA = new Path(_inputPath, "foo");
    writeFile(outputA, "bar");

    PathUtils.getSchemaFromFile(getFileSystem(), outputA);
  }

  @Test(expectedExceptions = IOException.class)
  public void getSchemaFromPathMissingTest() throws IOException {
    PathUtils.getSchemaFromPath(getFileSystem(), new Path("foo"));
  }

  @Test(expectedExceptions = IOException.class)
  public void getSchemaFromFileMissingTest() throws IOException {
    PathUtils.getSchemaFromFile(getFileSystem(), new Path("foo"));
  }

  @Test(expectedExceptions = IOException.class)
  public void getSchemaFromPathEmptyTest() throws IOException {
    PathUtils.getSchemaFromPath(getFileSystem(), _inputPath);
  }

  @Test
  public void countBytesEmptyTest() throws IOException {
    Assert.assertEquals(PathUtils.countBytes(getFileSystem(), _inputPath), 0);
  }

  @Test
  public void countBytesTest() throws IOException {
    Path filePath = new Path(_inputPath, "foo");
    FSDataOutputStream fin = getFileSystem().create(filePath);
    fin.writeUTF("bar");
    fin.close();

    Assert.assertEquals(PathUtils.countBytes(getFileSystem(), _inputPath), getFileSystem().getFileStatus(filePath)
        .getLen());
  }

  @Test(expectedExceptions = IOException.class)
  public void countBytesMissingTest() throws IOException {
    PathUtils.countBytes(getFileSystem(), new Path("foo"));
  }

  @Test(expectedExceptions = IOException.class)
  public void countBytesNotDirectoryTest() throws IOException {
    Path filePath = new Path(_inputPath, "foo");
    FSDataOutputStream fin = getFileSystem().create(filePath);
    fin.writeUTF("bar");
    fin.close();

    PathUtils.countBytes(getFileSystem(), filePath);
  }

  @Test
  public void getPathsInDirectoryEmptyTest() throws IOException {
    Assert.assertEquals(PathUtils.getPathsInDirectory(getFileSystem(), _inputPath).size(), 0);
  }

  @Test
  public void getPathsInDirectoryTest() throws IOException {
    Path outputA = new Path(_inputPath, "foo");
    writeFile(outputA, "bar");

    List<Path> l = PathUtils.getPathsInDirectory(getFileSystem(), _inputPath);
    Assert.assertEquals(l.size(), 1);
    Assert.assertEquals(l.get(0).toString(), outputA.toString());
  }

  @Test(expectedExceptions = IOException.class)
  public void getPathsInDirectoryMissingTest() throws IOException {
    PathUtils.getPathsInDirectory(getFileSystem(), new Path("foo"));
  }

  @Test
  public void getDateForPathTest() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String dateStr = "2014-01-01";
    Path p = new Path(_inputPath, dateStr);
    Date date = PathUtils.getDateForPath(p, sdf);
    try {
      Assert.assertEquals(date, sdf.parse(dateStr));
    } catch (ParseException e) {
      Assert.fail();
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void getDateForPathInvalidTest() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Path p = new Path(_inputPath, "foo");
    PathUtils.getDateForPath(p, sdf);
  }

  @Test
  public void nestedPathTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    Path pathA = new Path(_inputPath, "2014/01/03");
    writeFile(pathA, "");
    Path pathB = new Path(_inputPath, "2014/01/01");
    writeFile(pathB, "");
    Path pathC = new Path(_inputPath, "2013/01/01");
    writeFile(pathC, "");

    List<DatePath> datePaths = PathUtils.findNestedDatedPaths(getFileSystem(), _inputPath, sdf);
    Assert.assertEquals(datePaths.size(), 3);
    Assert.assertEquals(sdf.format(datePaths.get(0).getDate()), "2013/01/01");
    Assert.assertEquals(sdf.format(datePaths.get(1).getDate()), "2014/01/01");
    Assert.assertEquals(sdf.format(datePaths.get(2).getDate()), "2014/01/03");
  }

  @Test
  public void getRootForDatePathTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    Path path = new Path(_inputPath, "2014/01/03");

    Path p = PathUtils.getRootForDatePath(path, sdf);
    Assert.assertEquals(p.toString(), _inputPath.toString());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void getRootForDatePathMalformedTest() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
    Path path = new Path(_inputPath, "foo");

    PathUtils.getRootForDatePath(path, sdf);
  }

  //UTILITY
  private void writeFile(Path path, String content) throws IOException {
    FileSystem _fs = getFileSystem();
    _fs.mkdirs(path);

    _log.info("*** Write file in " + path);
    Path filePath = new Path(path, "part-00000");
    FSDataOutputStream fin = _fs.create(filePath);
    fin.writeUTF(content);
    fin.close();

    Assert.assertTrue(_fs.exists(filePath));
  }
}
