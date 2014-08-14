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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.VersionInfo;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class TestBase {

  private static String LOG_DIR = "test-logs";

  private Logger _log = Logger.getLogger(TestBase.class);
  private MiniDFSCluster _dfsCluster;
  private MiniMRCluster _mrCluster;
  private FileSystem _fileSystem;

  private boolean localMR;
  private boolean localFS;

  private int taskTrackers;
  private int dataNodes;

  public TestBase() {
    this(1, 1);
  }

  public TestBase(int taskTrackers, int dataNodes) {
    if (System.getProperty("testlocal") != null && Boolean.parseBoolean(System.getProperty("testlocal"))) {
      localMR = true;
      localFS = true;
    }

    if (taskTrackers < 1) {
      throw new IllegalArgumentException("Invalid taskTrackers value, must be greater than 0");
    }
    if (dataNodes < 1) {
      throw new IllegalArgumentException("Invalid dataNodes value, must be greater than 0");
    }
    this.taskTrackers = taskTrackers;
    this.dataNodes = dataNodes;
  }

  @BeforeClass
  public void startCluster() throws IOException {
    // make sure the log folder exists or it will fail
    File logPath = new File(LOG_DIR);
    if (!logPath.exists()) {
      logPath.mkdirs();
    }

    System.setProperty("hadoop.log.dir", LOG_DIR);
    System.setProperty("javax.xml.parsers.SAXParserFactory",
        "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
    System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
        "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

    _log.info("*** Hadoop Version: " + VersionInfo.getVersion());
    if (localFS) {
      _fileSystem = FileSystem.get(new JobConf());
      _log.info("*** Using local file system: " + _fileSystem.getUri());
    } else {
      _log.info("*** Starting Mini DFS Cluster");
      _dfsCluster = new MiniDFSCluster(new JobConf(), dataNodes, true, null);
      _fileSystem = _dfsCluster.getFileSystem();
    }

    if (localMR) {
      _log.info("*** Using local MR Cluster");
    } else {
      _log.info("*** Starting Mini MR Cluster");
      _mrCluster = new MiniMRCluster(taskTrackers, _fileSystem.getUri().toString(), 1);
    }
  }

  @AfterClass
  public void stopCluster() throws IOException {
    if (_mrCluster != null) {
      _log.info("*** Shutting down Mini MR Cluster");
      _mrCluster.shutdown();
      _mrCluster = null;
    }
    if (_dfsCluster != null) {
      _log.info("*** Shutting down Mini DFS Cluster");
      _dfsCluster.shutdown();
      _dfsCluster = null;
      _fileSystem = null;
    }

    String baseTempDir = "build" + File.separator + "test" + File.separator;
    String dfsDir = baseTempDir + "data";
    FileUtils.deleteDirectory(new File(dfsDir));
    String mrDir = baseTempDir + "mapred";
    FileUtils.deleteDirectory(new File(mrDir));
    FileUtils.deleteDirectory(new File(LOG_DIR));
  }

  /**
   * Returns the Filesystem in use.
   *
   * TestCases should use this Filesystem as it is properly configured with the workingDir for
   * relative PATHs.
   *
   * @return the filesystem used by Hadoop.
   */
  protected FileSystem getFileSystem() {
    return _fileSystem;
  }

  /**
   * Returns a job configuration preconfigured to run against the Hadoop managed by the testcase.
   *
   * @return configuration that works on the testcase Hadoop instance
   */
  protected JobConf createJobConf() {
    JobConf conf;
    if (localMR) {
      conf = new JobConf();
      conf.set("mapred.job.tracker", "local");
      conf.set("fs.default.name", "file:///");
    } else {
      conf = _mrCluster.createJobConf();
    }
    conf.set("mapred.map.max.attempts", "1");
    conf.set("mapred.reduce.max.attempts", "1");
    conf.set("dfs.datanode.data.dir.perm", "775");
    return conf;
  }

  /**
   * Indicates if the MR is running in local or cluster mode.
   *
   * @return returns TRUE if the MR is running locally, FALSE if running in cluster mode.
   */
  public boolean isLocalMR() {
    return localMR;
  }

  /**
   * Indicates if the filesystem is local or DFS.
   *
   * @return returns TRUE if the filesystem is local, FALSE if it is DFS.
   */
  public boolean isLocalFS() {
    return localFS;
  }

  /**
   * Stores the configuration in the properties as "test.conf" so it can be used by the job. This
   * property is a special test hook to enable testing.
   *
   * @param props
   * @throws IOException
   */
  protected void storeTestConf(Properties props) throws IOException {
    ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(arrayOutputStream);
    createJobConf().write(dataStream);
    dataStream.flush();
    props.setProperty("test.conf", new String(Base64.encodeBase64(arrayOutputStream.toByteArray())));
  }

  /**
   * Creates properties object for testing. This contains test configuration that will be extracted
   * by the Hadoop jobs so the test cluster is used.
   *
   * @return properties
   * @throws IOException
   */
  protected Properties newTestProperties() throws IOException {
    Properties props = new Properties();
    storeTestConf(props);
    return props;
  }
}
