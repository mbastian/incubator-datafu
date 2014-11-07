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
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.mr.util.LatestExpansionFunction;


@Test(groups = "pcl")
public class TestLatestExpansionFunction extends TestBase {

  private final Logger _log = Logger.getLogger(TestLatestExpansionFunction.class);
  private final Path _inputPath = new Path("/input");
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  @BeforeMethod
  public void beforeMethod(Method method) throws IOException {
    _log.info("*** Running " + method.getName());

    _log.info("*** Cleaning input and output paths");
    getFileSystem().delete(_inputPath, true);
    getFileSystem().mkdirs(_inputPath);
  }

  @Test
  public void latestNoSuffixTest() {
    LatestExpansionFunction func = new LatestExpansionFunction(getFileSystem(), dateFormat, false, _log);
    String res = func.apply(_inputPath.toString());
    Assert.assertNotNull(res);
    Assert.assertEquals(res, _inputPath.toString());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void latestEmptyTest() {
    LatestExpansionFunction func = new LatestExpansionFunction(getFileSystem(), dateFormat, false, _log);
    String res = func.apply(_inputPath.toString() + "/" + func.getLatestSuffix());
    Assert.assertNotNull(res);
    Assert.assertEquals(res, _inputPath);
  }

  @Test
  public void latestDateTest() throws IOException {
    Path p1 = new Path(_inputPath, "2010-01-01");
    Path p2 = new Path(_inputPath, "2013-01-01");
    writeFile(p1, "");
    writeFile(p2, "");

    LatestExpansionFunction func = new LatestExpansionFunction(getFileSystem(), dateFormat, true, _log);
    String resPath = func.apply(_inputPath.toString() + "/" + func.getLatestSuffix());

    Assert.assertEquals(new Path(resPath), p2);
    Assert.assertTrue(getFileSystem().exists(p1));
    Assert.assertTrue(getFileSystem().exists(p2));
  }

  @Test
  public void latestLexicographicTest() throws IOException {
    Path p1 = new Path(_inputPath, "aaa");
    Path p2 = new Path(_inputPath, "zzz");
    writeFile(p1, "");
    writeFile(p2, "");

    LatestExpansionFunction func = new LatestExpansionFunction(getFileSystem(), dateFormat, false, _log);
    String resPath = func.apply(_inputPath.toString() + "/" + func.getLatestSuffix());

    Assert.assertEquals(new Path(resPath), p2);
    Assert.assertTrue(getFileSystem().exists(p1));
    Assert.assertTrue(getFileSystem().exists(p2));
  }

  @Test
  public void latestWithSuffixTest() throws IOException {
    Path p1 = new Path(_inputPath, "2010-01-01");
    Path p2 = new Path(_inputPath, "2013-01-01");
    writeFile(p1, "");
    writeFile(p2, "");

    LatestExpansionFunction func = new LatestExpansionFunction(getFileSystem(), dateFormat, true, _log);
    String resPath = func.apply(_inputPath.toString() + "/" + func.getLatestSuffix() + "#suffix");

    Assert.assertEquals(new Path(resPath), new Path(p2 + "#suffix"));
  }

  // UTILITY

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
