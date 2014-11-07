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

package datafu.mr.jobs;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import datafu.mr.fs.PathUtils;
import datafu.mr.util.DiscoveryHelper;
import datafu.mr.util.IntermediateTypeHelper;
import datafu.mr.util.LatestExpansionFunction;
import datafu.mr.util.ReduceEstimator;


/**
 * Base class for MapReduce jobs.
 *
 * <p>
 * This class defines a set of common methods and configuration shared by Hadoop jobs. Jobs can be
 * configured either by providing properties or by calling setters. Each property has a
 * corresponding setter.
 * </p>
 *
 * This class recognizes the following properties:
 *
 * <ul>
 * <li><em>input.path</em> - Input path job will read from</li>
 * <li><em>output.path</em> - Output path job will write to</li>
 * <li><em>temp.path</em> - Temporary path under which intermediate files are stored</li>
 * <li><em>counters.path</em> - Path to store job counters in</li>
 * <li><em>use.latest.expansion</em> - Expand input paths with #LATEST (date|default|disabled)</li>
 * <li><em>latest.expansion.date.format</em> - Expansion date format, default is yyyy-MM-dd</li>
 * <li><em>num.reducers.bytes.per.reducer</em> - Number of bytes per reducer used by the reduce
 * estimator</li>
 * </ul>
 *
 * <p>
 * In addition, the following Hadoop properties can either be set by the <em>Properties</em> given
 * to the constructor, by using the setter methods or by overriding the getter methods.
 * </p>
 *
 * <ul>
 * <li><em>mapred.reduce.tasks</em> - Number of reducers, see
 * <code>{@link Job#setNumReduceTasks(int)}</code></li>
 * <li><em>mapreduce.map.class</em> - Mapper class, see
 * <code>{@link Job#setMapperClass(Class)}</code></li>
 * <li><em>mapreduce.reduce.class</em> - Reducer class, see
 * <code>{@link Job#setReducerClass(Class)}</code></li>
 * <li><em>mapreduce.combine.class</em> - Combiner class, see
 * <code>{@link Job#setCombinerClass(Class)}</code></li>
 * <li><em>mapreduce.partitioner.class</em> - Partitioner class, see
 * <code>{@link Job#setPartitionerClass(Class)}</code></li>
 * <li><em>mapred.mapoutput.key.class</em> - Map output key class, see
 * <code>{@link Job#setMapOutputKeyClass(Class)}</code></li>
 * <li><em>mapred.mapoutput.value.class</em> - Map output value class, see
 * <code>{@link Job#setMapOutputValueClass(Class)}</code></li>
 * <li><em>mapred.output.key.class</em> - Output key class, see
 * <code>{@link Job#setOutputKeyClass(Class)}</code></li>
 * <li><em>mapred.output.value.class</em> - Output value class, see
 * <code>{@link Job#setOutputValueClass(Class)}</code></li>
 * <li><em>mapred.output.key.comparator.class</em> - Sort comparator class, see
 * <code>{@link Job#setSortComparatorClass(Class)}</code></li>
 * <li><em>mapred.output.value.groupfn.class</em> - Grouping comparator class, see
 * <code>{@link Job#setGroupingComparatorClass(Class)}</code></li>
 * <li><em>mapred.cache.files</em> - Distributed cache files separated by commas, see
 * <code>{@link DistributedCache}</code></li>
 * <li><em>mapred.job.classpath.files</em> - Distributed cache classpath files separated by commas, see
 * <code>{@link DistributedCache}</code></li>
 * </ul>
 *
 * <p>
 * The <em>input.path</em> property may be a comma-separated list of paths. When there is more than
 * one it implies a join is to be performed. Alternatively the paths may be listed separately. For
 * example, <em>input.path.first</em> and <em>input.path.second</em> define two separate input
 * paths.
 * </p>
 *
 * <p>
 * The <em>temp.path</em> property defines the parent directory for temporary paths, not the
 * temporary path itself. Temporary paths are created under this directory and suffixed with the
 * <em>output.path</em>. The default is <em>/tmp</em>
 * </p>
 *
 * <p>
 * The <em>use.latest.expansion</em> property defines whether to expand the input paths which
 * contain the <em>#LATEST</em> suffix. When set to <em>default</em> the folders are sorted
 * lexicographically and the last path is chosen. When set to <em>date</em> the folders are first
 * matched against the date format which can be configured with <em>latest.expansion.date.format</em>. Then,
 * the most recent path is chosen. When set to <em>disabled</em>, no expansion is performed. By default, the
 * <em>latest.expansion.date.format</em> value is <em>yyyy-MM-dd</em>. 
 * </p>
 *
 * <p>
 * The input and output paths are the only required parameters. The rest is optional.
 * </p>
 *
 * <p>
 * Three methods can be overridden to customize the execution flow:
 * <ul>
 * <li><em>init(): </em>Just after instantiation</li>
 * <li><em>configure(): </em>Before the job starts</li>
 * <li><em>finish(): </em>After the job ended (successfully)</li>
 * </ul>
 *
 * @author Mathieu Bastian
 */
public abstract class AbstractJob extends Configured {
  private static String HADOOP_PREFIX = "hadoop-conf.";

  private final Logger _log = Logger.getLogger(AbstractJob.class);

  private Properties props;
  private String name;
  private Path countersParentPath;
  private Integer numReducers;
  private List<Path> inputPaths;
  private Path outputPath;
  private Path tempPath = new Path("/tmp");
  private FileSystem fs;
  private List<Path> distributedCache;
  private List<Path> distributedCacheClasspath;
  private String useLatestExpansion = "default";
  private SimpleDateFormat latestExpansionDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  @SuppressWarnings("rawtypes")
  protected Class<? extends Mapper> mapperClass;
  @SuppressWarnings("rawtypes")
  protected Class<? extends Reducer> reducerClass;
  @SuppressWarnings("rawtypes")
  protected Class<? extends Reducer> combinerClass;
  @SuppressWarnings("rawtypes")
  protected Class<? extends Partitioner> partitionerClass;
  @SuppressWarnings("rawtypes")
  protected Class<? extends RawComparator> groupingComparatorClass;
  @SuppressWarnings("rawtypes")
  protected Class<? extends RawComparator> sortComparatorClass;
  protected Class<?> mapOutputKeyClass;
  protected Class<?> mapOutputValueClass;
  protected Class<?> outputKeyClass;
  protected Class<?> outputValueClass;

  /**
   * Initializes the job.
   */
  public AbstractJob() {
    setConf(new Configuration());
    setName(getClass().getSimpleName());
  }

  /**
   * Initializes the job with a job name and properties.
   *
   * @param name
   *          Job name
   * @param props
   *          Configuration properties
   */
  public AbstractJob(String name, Properties props) {
    this();
    setName(name);
    setProperties(props);
  }

  /**
   * Gets the job name
   *
   * @return Job name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the job name
   *
   * @param name
   *          Job name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets the configuration properties.
   *
   * @return Configuration properties
   */
  public Properties getProperties() {
    return props;
  }

  /**
   * Sets the configuration properties.
   *
   * @param props
   *          Properties
   */
  public void setProperties(Properties props) {
    this.props = props;
    updateConfigurationFromProps(props);

    if (props.containsKey("input.path")) {
      String[] pathSplit = props.getProperty("input.path").split(",");
      List<Path> paths = new ArrayList<Path>();
      for (String path : pathSplit) {
        if (path != null && path.length() > 0) {
          path = path.trim();
          if (path.length() > 0) {
            paths.add(new Path(path));
          }
        }
      }
      if (paths.size() > 0) {
        setInputPaths(paths);
      } else {
        throw new RuntimeException("Could not extract input paths from: " + props.get("input.path"));
      }
    } else {
      List<Path> inputPaths = new ArrayList<Path>();
      for (Object o : props.keySet()) {
        String prop = o.toString();
        if (prop.startsWith("input.path.")) {
          inputPaths.add(new Path(props.getProperty(prop)));
        }
      }
      if (inputPaths.size() > 0) {
        setInputPaths(inputPaths);
      }
    }

    if (props.containsKey("output.path")) {
      setOutputPath(new Path(props.getProperty("output.path")));
    }

    if (props.containsKey("temp.path")) {
      setTempPath(new Path(props.getProperty("temp.path")));
    }

    if (props.containsKey("counters.path")) {
      setCountersParentPath(new Path(props.getProperty("counters.path")));
    }

    if (props.containsKey("mapred.reduce.tasks")) {
      setNumReducers(Integer.parseInt(props.getProperty("mapred.reduce.tasks")));
    }

    if (props.containsKey("mapred.cache.files")) {
      String[] pathSplit = props.getProperty("mapred.cache.files").split(",");
      List<Path> paths = new ArrayList<Path>();
      for (String path : pathSplit) {
        if (path != null && path.length() > 0) {
          path = path.trim();
          if (path.length() > 0) {
            paths.add(new Path(path));
          }
        }
      }
      if (paths.size() > 0) {
        setDistributedCachePaths(paths);
      } else {
        throw new RuntimeException("Could not extract distributed cache paths from: " + props.get("mapred.cache.files"));
      }
    }

    if (props.containsKey("mapred.job.classpath.files")) {
      String[] pathSplit = props.getProperty("mapred.job.classpath.files").split(",");
      List<Path> paths = new ArrayList<Path>();
      for (String path : pathSplit) {
        if (path != null && path.length() > 0) {
          path = path.trim();
          if (path.length() > 0) {
            paths.add(new Path(path));
          }
        }
      }
      if (paths.size() > 0) {
        setDistributedCacheClasspaths(paths);
      } else {
        throw new RuntimeException("Could not extract distributed classpath cache paths from: "
            + props.get("mapred.job.classpath.files"));
      }
    }

    if (props.containsKey("mapreduce.mapper.class")) {
      setMapperClass(getConf().getClass(props.getProperty("mapreduce.mapper.class"), null, Mapper.class));
    }

    if (props.containsKey("mapreduce.reducer.class")) {
      setReducerClass(getConf().getClass(props.getProperty("mapreduce.reducer.class"), null, Reducer.class));
    }

    if (props.containsKey("mapreduce.combine.class")) {
      setCombinerClass(getConf().getClass(props.getProperty("mapreduce.combine.class"), null, Reducer.class));
    }

    if (props.containsKey("mapreduce.partitioner.class")) {
      setPartitionerClass(getConf().getClass(props.getProperty("mapreduce.partitioner.class"), null, Partitioner.class));
    }

    if (props.containsKey("mapred.mapoutput.key.class")) {
      setMapOutputKeyClass(getConf().getClass(props.getProperty("mapred.mapoutput.key.class"), null));
    }

    if (props.containsKey("mapred.mapoutput.value.class")) {
      setMapOutputValueClass(getConf().getClass(props.getProperty("mapred.mapoutput.value.class"), null));
    }

    if (props.containsKey("mapred.output.key.class")) {
      setOutputKeyClass(getConf().getClass(props.getProperty("mapred.output.key.class"), null));
    }

    if (props.containsKey("mapred.output.value.class")) {
      setOutputValueClass(getConf().getClass(props.getProperty("mapred.output.value.class"), null));
    }

    if (props.containsKey("mapred.output.key.comparator.class")) {
      setSortComparatorClass(getConf().getClass(props.getProperty("mapred.output.key.comparator.class"), null,
          RawComparator.class));
    }

    if (props.containsKey("mapred.output.value.groupfn.class")) {
      setGroupingComparatorClass(getConf().getClass(props.getProperty("mapred.output.value.groupfn.class"), null,
          RawComparator.class));
    }

    if (props.get("use.latest.expansion") != null) {
      setUseLatestExpansion(props.getProperty("use.latest.expansion"));
    }

    if (props.get("latest.expansion.date.format") != null) {
      setLatestExpansionDateFormat(props.getProperty("latest.expansion.date.format"));
    }
    _log.info(String.format("Using latest expansion: %s (format is %s)", useLatestExpansion,
        latestExpansionDateFormat.toPattern()));
  }

  /**
   * Overridden to provide custom configuration after instantiation
   *
   * @param conf the configuration
   */
  public void init(Configuration conf) {
  }

  /**
   * Overridden to provide custom configuration before the job starts.
   *
   * @param job the job
   */
  public void configure(Job job) {
  }

  /**
   * Overridden to provide custom actions after the job finishes.
   *
   * @param job the job
   */
  public void finish(Job job) {
  }

  /**
   * Gets the number of reducers to use.
   *
   * @return Number of reducers
   */
  public Integer getNumReducers() {
    return numReducers;
  }

  /**
   * Sets the number of reducers to use. Can also be set with <em>num.reducers</em> property.
   *
   * @param numReducers
   *          Number of reducers to use
   */
  public void setNumReducers(Integer numReducers) {
    this.numReducers = numReducers;
  }

  /**
   * Gets the path where counters will be stored.
   *
   * @return Counters path
   */
  public Path getCountersParentPath() {
    return countersParentPath;
  }

  /**
   * Returns true if the latest expansion is in use.
   *
   * @return true if latest expansion is enabled
   */
  public boolean isUseLatestExpansion() {
    return !useLatestExpansion.equals("disabled");
  }

  /**
   * Gets the latest expansion configuration.
   *
   * @return latest expansion flag
   */
  public String getUseLatestExpansion() {
    return useLatestExpansion;
  }

  /**
   * Sets the latest expansion setting (date|default|disabled).
   *
   * @param useLatestExpansion
   *          Use latest expansion
   */
  public void setUseLatestExpansion(String useLatestExpansion) {
    if (!(useLatestExpansion.equals("disabled") || useLatestExpansion.equals("date") || useLatestExpansion
        .equals("default"))) {
      throw new IllegalArgumentException("The 'use.latest.expansion' setting must be 'default', 'date', or 'disabled'");
    }
    this.useLatestExpansion = useLatestExpansion;
  }

  /**
   * Sets the path where counters will be stored. Can also be set with <em>counters.path</em>.
   *
   * @param countersParentPath
   *          Counters path
   */
  public void setCountersParentPath(Path countersParentPath) {
    this.countersParentPath = countersParentPath;
  }

  /**
   * Sets the latest expansion date format.
   *
   * @param latestExpansionDateFormat date format
   */
  public void setLatestExpansionDateFormat(String latestExpansionDateFormat) {
    this.latestExpansionDateFormat = new SimpleDateFormat(latestExpansionDateFormat);
    ;
  }

  /**
   * Gets the latest expansion date format.
   *
   * @return latest expansion date format
   */
  public SimpleDateFormat getLatestExpansionDateFormat() {
    return latestExpansionDateFormat;
  }

  /**
   * Gets the input paths. Multiple input paths imply a join is to be performed.
   *
   * @return input paths
   */
  public List<Path> getInputPaths() {
    return inputPaths;
  }

  /**
   * Sets the input paths. Multiple input paths imply a join is to be performed. Can also be set
   * with <em>input.path</em> or several properties starting with <em>input.path.</em>.
   *
   * @param inputPaths
   *          input paths
   */
  public void setInputPaths(List<Path> inputPaths) {
    this.inputPaths = inputPaths;
  }

  /**
   * Sets the distributed cache paths.
   *
   * @param cachePaths cache paths
   */
  public void setDistributedCachePaths(List<Path> cachePaths) {
    this.distributedCache = cachePaths;
  }

  /**
   * Returns the distributed cache paths
   *
   * @return the list of distributed cache paths
   */
  public List<Path> getDistributedCachePaths() {
    return distributedCache;
  }

  /**
   * Sets the distributed cache classpaths.
   *
   * @param classpathCachePaths classpath paths
   */
  public void setDistributedCacheClasspaths(List<Path> classpathCachePaths) {
    this.distributedCacheClasspath = classpathCachePaths;
  }

  /**
   * Returns the distributed cache classpaths.
   *
   * @return the list of distributed classpath cache paths
   */
  public List<Path> getDistributedCacheClasspaths() {
    return distributedCacheClasspath;
  }

  /**
   * Gets the output path.
   *
   * @return output path
   */
  public Path getOutputPath() {
    return outputPath;
  }

  /**
   * Sets the output path. Can also be set with <em>output.path</em>.
   *
   * @param outputPath
   *          output path
   */
  public void setOutputPath(Path outputPath) {
    this.outputPath = outputPath;
  }

  /**
   * Gets the temporary path under which intermediate files will be stored. Defaults to /tmp.
   *
   * @return Temporary path
   */
  public Path getTempPath() {
    return tempPath;
  }

  /**
   * Sets the temporary path where intermediate files will be stored. Defaults to /tmp.
   *
   * @param tempPath
   *          Temporary path
   */
  public void setTempPath(Path tempPath) {
    this.tempPath = tempPath;
  }

  /**
   * Gets the file system.
   *
   * @return File system
   */
  protected FileSystem getFileSystem() {
    if (fs == null) {
      try {
        fs = FileSystem.get(getConf());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return fs;
  }

  /**
   * Generates a random temporary path within the file system. This does not create the path.
   *
   * @return Random temporary path
   */
  protected Path randomTempPath() {
    return new Path(tempPath, String.format("mr-%s", UUID.randomUUID()));
  }

  /**
   * Creates a random temporary path within the file system.
   *
   * @return Random temporary path
   * @throws IOException IOException
   */
  protected Path createRandomTempPath() throws IOException {
    return ensurePath(randomTempPath());
  }

  /**
   * Creates a path, if it does not already exist.
   *
   * @param path
   *          Path to create
   * @return The same path that was provided
   * @throws IOException IOException
   */
  protected Path ensurePath(Path path) throws IOException {
    if (!getFileSystem().exists(path)) {
      getFileSystem().mkdirs(path);
    }
    return path;
  }

  /**
   * Setup the job input format.
   * <p>
   * One can use <code>job.setInputFormatClass()</code> to configure the job's input format.
   *
   * @param job
   *          the Hadoop job
   * @throws IOException
   *           when the configuration is throwing an error
   */
  protected void setupInputFormat(Job job) throws IOException {
  }

  /**
   * Setup the job output format.
   * <p>
   * One can use <code>job.setOutputFormatClass()</code> to configure the job's input format.
   *
   * @param job
   *          the Hadoop job
   * @throws IOException
   *           when the configuration is throwing an error
   */
  protected void setupOutputFormat(Job job) throws IOException {
  }

  /**
   * Gets the mapper class
   *
   * @return mapper class
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Mapper> getMapperClass() {
    return mapperClass;
  }

  /**
   * Sets the mapper class
   *
   * @param mapperClass
   *          mapper class
   */
  @SuppressWarnings("rawtypes")
  public void setMapperClass(Class<? extends Mapper> mapperClass) {
    this.mapperClass = mapperClass;
  }

  /**
   * Gets the reducer class
   *
   * @return reducer class
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Reducer> getReducerClass() {
    return reducerClass;
  }

  /**
   * Sets the reducer class
   *
   * @param reducerClass
   *          reducer class
   */
  @SuppressWarnings("rawtypes")
  public void setReducerClass(Class<? extends Reducer> reducerClass) {
    this.reducerClass = reducerClass;
  }

  /**
   * Gets the combiner class
   *
   * @return combiner class
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Reducer> getCombinerClass() {
    return combinerClass;
  }

  /**
   * Sets the combiner class
   *
   * @param combinerClass
   *          combiner class
   */
  @SuppressWarnings("rawtypes")
  public void setCombinerClass(Class<? extends Reducer> combinerClass) {
    this.combinerClass = combinerClass;
  }

  /**
   * Gets the partitioner class
   *
   * @return partitioner class
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Partitioner> getPartitionerClass() {
    return partitionerClass;
  }

  /**
   * Sets the partitioner class
   *
   * @param partitionerClass
   *          partitioner class
   */
  @SuppressWarnings("rawtypes")
  public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) {
    this.partitionerClass = partitionerClass;
  }

  /**
   * Gets the grouping comparator
   *
   * @return grouping comparator
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends RawComparator> getGroupingComparator() {
    return groupingComparatorClass;
  }

  /**
   * Sets the grouping comparator class
   *
   * @param groupingComparatorClass
   *          grouping comparator class
   */
  @SuppressWarnings("rawtypes")
  public void setGroupingComparatorClass(Class<? extends RawComparator> groupingComparatorClass) {
    this.groupingComparatorClass = groupingComparatorClass;
  }

  /**
   * Gets the sort comparator
   *
   * @return sort comparator
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends RawComparator> getSortComparator() {
    return sortComparatorClass;
  }

  /**
   * Sets the sort comparator
   *
   * @param sortComparatorClass
   *          sort comparator class
   */
  @SuppressWarnings("rawtypes")
  public void setSortComparatorClass(Class<? extends RawComparator> sortComparatorClass) {
    this.sortComparatorClass = sortComparatorClass;
  }

  /**
   * Gets the map output key class.
   *
   * @return map output key class
   */
  public Class<?> getMapOutputKeyClass() {
    return mapOutputKeyClass;
  }

  /**
   * Sets the map output key class
   *
   * @param mapOutputKeyClass
   *          map output key class
   */
  public void setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
    this.mapOutputKeyClass = mapOutputKeyClass;
  }

  /**
   * Gets the map output value class
   *
   * @return map output value class
   */
  public Class<?> getMapOutputValueClass() {
    return mapOutputValueClass;
  }

  /**
   * Sets the map output value class
   *
   * @param mapOutputValueClass
   *          map output value class
   */
  public void setMapOutputValueClass(Class<?> mapOutputValueClass) {
    this.mapOutputValueClass = mapOutputValueClass;
  }

  /**
   * Gets the reduce output key class.
   *
   * @return reduce output key class
   */
  public Class<?> getOutputKeyClass() {
    return outputKeyClass;
  }

  /**
   * Sets the output reduce key class
   *
   * @param outputKeyClass
   *          reduce output key class
   */
  public void setOutputKeyClass(Class<?> outputKeyClass) {
    this.outputKeyClass = outputKeyClass;
  }

  /**
   * Gets the reduce output value class
   *
   * @return reduce output value class
   */
  public Class<?> getOutputValueClass() {
    return outputValueClass;
  }

  /**
   * Sets the output reduce value class
   *
   * @param outputValueClass
   *          reduce output value class
   */
  public void setOutputValueClass(Class<?> outputValueClass) {
    this.outputValueClass = outputValueClass;
  }

  /**
   * Run the job.
   *
   * @throws IOException IOException
   * @throws InterruptedException InterruptedException
   * @throws ClassNotFoundException ClassNotFoundException
   */
  @SuppressWarnings("rawtypes")
  public void run() throws IOException, InterruptedException, ClassNotFoundException {
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      getConf().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
      _log.info(String.format("Set 'mapreduce.job.credentials.binary' to %s",
          System.getenv("HADOOP_TOKEN_FILE_LOCATION")));
    }

    init(getConf());

    List<Path> inputPaths = getInputPaths();
    if (inputPaths == null) {
      throw new RuntimeException("Input path is not specified. Setup the 'input.path' parameter.");
    }

    LatestExpansionFunction latestExpansionFunction =
        new LatestExpansionFunction(getFileSystem(), getLatestExpansionDateFormat(), getUseLatestExpansion().equals(
            "date"), _log);
    List<String> inputPathsStr = new ArrayList<String>();
    for (Path p : inputPaths) {
      String ip = isUseLatestExpansion() ? latestExpansionFunction.apply(p.toString()) : p.toString();
      inputPathsStr.add(ip);
      _log.info(String.format("Adding %s to the input paths", ip));
    }

    if (inputPathsStr.isEmpty()) {
      throw new RuntimeException("No input paths can be found");
    }

    Path outputPath = getOutputPath();

    if (outputPath == null) {
      throw new RuntimeException("Output path is not specified. Setup the 'output.path' parameter.");
    }

    if (getDistributedCachePaths() != null) {
      List<Path> paths = new ArrayList<Path>();
      boolean useSymlink = false;
      for (Path p : getDistributedCachePaths()) {
        Path dpath = isUseLatestExpansion() ? new Path(latestExpansionFunction.apply(p.toString())) : p;
        paths.add(dpath);
        useSymlink |= dpath.toString().contains("#");
        _log.info(String.format("Adding %s to the distributed cache", dpath.toString()));
      }

      String dCachePaths = StringUtils.join(PathUtils.getPathsToString(paths).iterator(), ",");
      getConf().set("mapred.cache.files", dCachePaths);
      _log.info(String.format("Set 'mapred.cache.files' to %s", dCachePaths));

      if (useSymlink) {
        _log.info("Symlink detected, set 'mapred.create.symlink' to 'yes'");
        getConf().set("mapred.create.symlink", "yes");
      }
      setDistributedCachePaths(paths);
    }

    if (getDistributedCacheClasspaths() != null) {
      List<Path> paths = new ArrayList<Path>();
      for (Path p : getDistributedCacheClasspaths()) {
        if (getFileSystem().exists(p)) {
          FileStatus fs = getFileSystem().getFileStatus(p);
          if (fs.isDir()) {
            paths.addAll(PathUtils.getPathsInDirectory(getFileSystem(), p));
          } else {
            paths.add(p);
          }
        } else {
          throw new RuntimeException(String.format("The classpath cache file %s can't be found", p.getName()));
        }
      }
      for (Path dpath : paths) {
        _log.info(String.format("Adding %s to the distributed cache classpath", dpath.toString()));
        DistributedCache.addFileToClassPath(dpath, getConf(), getFileSystem());
      }

      setDistributedCacheClasspaths(paths);
    }

    final StagedOutputJob job =
        StagedOutputJob.createStagedJob(getConf(), getName(), inputPathsStr, tempPath.toString(),
            outputPath.toString(), _log);

    Class<? extends Mapper> mapperClass = DiscoveryHelper.getMapperClass(this);
    if (mapperClass == null) {
      throw new RuntimeException("No mapper class implementation is defined. Override the 'getMapperClass()' method.");
    }
    job.setMapperClass(mapperClass);

    Class<? extends Reducer> reducerClass = DiscoveryHelper.getReducerClass(this);
    if (reducerClass != null) {
      job.setReducerClass(reducerClass);

      int numReducers;
      if (getNumReducers() != null) {
        numReducers = getNumReducers();
        _log.info(String.format("Using %d reducers (fixed)", numReducers));
      } else {
        ReduceEstimator estimator = new ReduceEstimator(getFileSystem(), getProperties());
        numReducers = estimator.getNumReducers();
        _log.info(String.format("Using %d reducers (computed)", numReducers));
      }

      job.setNumReduceTasks(numReducers);
    } else {
      job.setNumReduceTasks(0);
      _log.info("Using 0 reducers (map-only)");
    }

    if (getCombinerClass() != null) {
      job.setCombinerClass(getCombinerClass());
      _log.info(String.format("Using %s as combiner", getCombinerClass().getSimpleName()));
    }

    if (getPartitionerClass() != null) {
      job.setPartitionerClass(getPartitionerClass());
      _log.info(String.format("Using %s as partitioner", getPartitionerClass().getSimpleName()));
    }

    if (getMapOutputKeyClass() != null) {
      job.setMapOutputKeyClass(getMapOutputKeyClass());
      _log.info(String.format("Using %s as map output key class", getMapOutputKeyClass().getSimpleName()));
    } else {
      Class<?> keyClass = IntermediateTypeHelper.getMapperOutputKeyClass(mapperClass);
      if (keyClass != null) {
        job.setMapOutputKeyClass(keyClass);
        _log.info(String.format("Discovered map output key class: %s", keyClass.getName()));
      } else {
        _log.warn("Could not discover the map output key class");
      }
    }

    if (getMapOutputValueClass() != null) {
      job.setMapOutputValueClass(getMapOutputValueClass());
      _log.info(String.format("Using %s as map output value class", getMapOutputValueClass().getSimpleName()));
    } else {
      Class<?> valueClass = IntermediateTypeHelper.getMapperOutputValueClass(mapperClass);
      if (valueClass != null) {
        job.setMapOutputValueClass(valueClass);
        _log.info(String.format("Discovered map output value class: %s", valueClass.getName()));
      } else {
        _log.warn("Could not discover the map output value class");
      }
    }

    if (reducerClass != null) {
      if (getOutputKeyClass() != null) {
        job.setOutputKeyClass(getOutputKeyClass());
        _log.info(String.format("Using %s as output key class", getOutputKeyClass().getSimpleName()));
      } else {
        Class<?> keyClass = IntermediateTypeHelper.getReducerOutputKeyClass(reducerClass);
        if (keyClass != null) {
          job.setOutputKeyClass(keyClass);
          _log.info(String.format("Discovered reducer output key class: %s", keyClass.getName()));
        } else {
          _log.warn("Could not discover the reduce output key class");
        }
      }

      if (getOutputValueClass() != null) {
        job.setOutputValueClass(getOutputValueClass());
        _log.info(String.format("Using %s as output value class", getOutputValueClass().getSimpleName()));
      } else {
        Class<?> valueClass = IntermediateTypeHelper.getReducerOutputValueClass(reducerClass);
        if (valueClass != null) {
          job.setOutputValueClass(valueClass);
          _log.info(String.format("Discovered reducer output value class: %s", valueClass.getName()));
        } else {
          _log.warn("Could not discover the reduce output value class");
        }
      }
    } else {
      job.setOutputKeyClass(job.getMapOutputKeyClass());
      _log.info(String.format("Using %s as output key class (map-only)", job.getMapOutputKeyClass().getSimpleName()));

      job.setOutputValueClass(job.getMapOutputValueClass());
      _log.info(String
          .format("Using %s as output value class (map-only)", job.getMapOutputValueClass().getSimpleName()));
    }

    setupInputFormat(job);
    setupOutputFormat(job);

    if (getGroupingComparator() != null) {
      job.setGroupingComparatorClass(getGroupingComparator());
      _log.info(String.format("Using %s as grouping comparator", getGroupingComparator().getSimpleName()));
    }

    if (getSortComparator() != null) {
      job.setSortComparatorClass(getSortComparator());
      _log.info(String.format("Using %s as sort comparator", getSortComparator().getSimpleName()));
    }

    configure(job);

    if (!job.waitForCompletion(true)) {
      _log.error("Job failed! Quitting...");
      throw new RuntimeException("Job failed");
    }

    finish(job);
  }

  /**
   * Creates Hadoop configuration using the provided properties.
   * 
   * @param props
   * @return
   */
  private void updateConfigurationFromProps(Properties props) {
    Configuration config = getConf();

    if (config == null) {
      config = new Configuration();
      setConf(config);
    }

    // to enable unit tests to inject configuration
    if (props.containsKey("test.conf")) {
      try {
        byte[] decoded = Base64.decodeBase64(props.getProperty("test.conf"));
        ByteArrayInputStream byteInput = new ByteArrayInputStream(decoded);
        DataInputStream inputStream = new DataInputStream(byteInput);
        config.readFields(inputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    for (String key : props.stringPropertyNames()) {
      String newKey = key;
      String value = props.getProperty(key);

      if (key.toLowerCase().startsWith(HADOOP_PREFIX)) {
        newKey = key.substring(HADOOP_PREFIX.length());
        config.set(newKey, value);
        props.remove(key);
        props.setProperty(newKey, value);
      } else {
        config.set(key, value);
      }
    }
  }
}
