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

package datafu.mr.fs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;


/**
 * A collection of utility methods for dealing with files and paths.
 *
 * @author Matthew Hayes
 */
public class PathUtils {
  private static Logger _log = Logger.getLogger(PathUtils.class);

  /**
   * Filters out paths starting with "." and "_".
   */
  public static final PathFilter nonHiddenPathFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String s = path.getName().toString();
      return !s.startsWith(".") && !s.startsWith("_");
    }
  };

  /**
   * Delete all but the last N paths matching the given date format.
   *
   * @param fs the file system
   * @param path the path
   * @param retentionCount the number of dated paths to retain
   * @param dateFormat the date format
   * @throws IOException IOException
   */
  public static void keepLatestDatedPaths(FileSystem fs, Path path, int retentionCount, SimpleDateFormat dateFormat)
      throws IOException {
    if (retentionCount < 1) {
      throw new IllegalArgumentException("The retention count should be a positive number");
    }
    LinkedList<DatePath> outputs = new LinkedList<DatePath>(PathUtils.findDatedPaths(fs, path, dateFormat));
    _log.info(String.format("Found %d matching folders in %s", outputs.size(), path.toString()));

    while (outputs.size() > retentionCount) {
      DatePath toDelete = outputs.removeFirst();
      _log.info(String.format("Removing %s", toDelete.getPath()));
      fs.delete(toDelete.getPath(), true);
    }
  }

  /**
   * Delete all but the last N days of paths matching the given format.
   *
   * @param fs the file system
   * @param path the path
   * @param retentionCount the number of dated paths to retain
   * @param format the date format
   * @throws IOException IOException
   */
  public static void keepLatestNestedDatedPaths(FileSystem fs, Path path, int retentionCount, SimpleDateFormat format)
      throws IOException {
    if (retentionCount < 1) {
      throw new IllegalArgumentException("The retention count should be a positive number");
    }
    LinkedList<DatePath> outputs = new LinkedList<DatePath>(PathUtils.findNestedDatedPaths(fs, path, format));
    _log.info(String.format("Found %d matching folders in %s", outputs.size(), path.toString()));

    while (outputs.size() > retentionCount) {
      DatePath toDelete = outputs.removeFirst();
      _log.info(String.format("Removing %s", toDelete.getPath()));
      fs.delete(toDelete.getPath(), true);
    }
  }

  /**
   * List all paths matching the given format under a given path.
   * <p>
   * The format must be a nested format with slashes.
   *
   * @param fs
   *          file system
   * @param input
   *          path to search under
   * @param format the date format
   * @return paths list of dated paths
   * @throws IOException IOException
   */
  public static List<DatePath> findNestedDatedPaths(FileSystem fs, Path input, SimpleDateFormat format)
      throws IOException {
    if (!format.toPattern().contains("/")) {
      throw new RuntimeException("The format must contain at least one slash");
    }

    List<DatePath> outputs = new ArrayList<DatePath>();

    FileStatus[] pathsStatus =
        fs.globStatus(new Path(input, format.toPattern().replaceAll("[^/]+", "*")), nonHiddenPathFilter);

    if (pathsStatus == null) {
      return outputs;
    }

    for (FileStatus pathStatus : pathsStatus) {
      Date date;
      try {
        date = getDateForPath(pathStatus.getPath(), format);
      } catch (Exception e) {
        continue;
      }
      outputs.add(new DatePath(date, pathStatus.getPath(), format));
    }

    Collections.sort(outputs);

    return outputs;
  }

  /**
   * List all paths matching the given date format under a given path.
   * <p>
   * The output list is sorted ascending with the oldest date first.
   * @param fs
   *          file system
   * @param path
   *          path to search under
   * @param format
   *          the date format
   * @return paths list of dated paths
   * @throws IOException IOException
   */
  public static List<DatePath> findDatedPaths(FileSystem fs, Path path, SimpleDateFormat format) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException("The path doesn't exist");
    }
    FileStatus[] outputPaths = fs.listStatus(path, nonHiddenPathFilter);

    List<DatePath> outputs = new ArrayList<DatePath>();

    if (outputPaths != null) {
      for (FileStatus outputPath : outputPaths) {
        Date date;
        try {
          date = format.parse(outputPath.getPath().getName());
        } catch (ParseException e) {
          continue;
        }

        outputs.add(new DatePath(date, outputPath.getPath(), format));
      }
    }

    Collections.sort(outputs);

    return outputs;
  }

  /**
   * Gets the schema from a given Avro data file.
   *
   * @param fs the file system
   * @param path path to fetch schema for
   * @return The schema read from the data file's metadata.
   * @throws IOException IOException
   */
  public static Schema getSchemaFromFile(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException("The path " + path.toString() + " doesn't exist");
    }
    try {
      FSDataInputStream dataInputStream = fs.open(path);
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(dataInputStream, reader);
      try {
        return dataFileStream.getSchema();
      } finally {
        dataFileStream.close();
      }
    } catch (Exception e) {
      throw new IOException("The file couldn't be opened or is not Avro");
    }
  }

  /**
   * Gets the schema for the first Avro file under the given path.
   *
   * @param fs the file system
   * @param path
   *          path to fetch schema for
   * @return Avro schema
   * @throws IOException IOException
   */
  public static Schema getSchemaFromPath(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException("The path " + path.toString() + " doesn't exist");
    }
    FileStatus[] listStatus = fs.listStatus(path, nonHiddenPathFilter);
    if (listStatus.length == 0) {
      throw new IOException("The path " + path.toString() + " is empty");
    }
    for (FileStatus fileStatus : listStatus) {
      try {
        return getSchemaFromFile(fs, fileStatus.getPath());
      } catch (Exception e) {
        continue;
      }
    }
    throw new IOException("No Avro file was found in " + path.toString());
  }

  /**
   * Sums the size of all files listed under a given path.
   *
   * @param fs
   *          file system
   * @param path
   *          path to count bytes for
   * @return total bytes under path
   * @throws IOException IOException
   */
  public static long countBytes(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException("The path " + path.toString() + " doesn't exist");
    }
    if (!fs.getFileStatus(path).isDir()) {
      throw new IOException("The path is not a directory");
    }
    FileStatus[] files = fs.listStatus(path, nonHiddenPathFilter);
    long totalForPath = 0L;
    for (FileStatus file : files) {
      totalForPath += file.getLen();
    }
    return totalForPath;
  }

  /**
   * Returns all paths within a directory.
   *
   * @param fs file system
   * @param path directory path
   * @return list of paths within the directory, if any
   * @throws IOException IOException
   */
  public static List<Path> getPathsInDirectory(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
      throw new IOException("The path " + path.toString() + " doesn't exist");
    }
    List<Path> paths = new ArrayList<Path>();
    FileStatus fileStatus = fs.getFileStatus(path);
    if (!fileStatus.isDir()) {
      throw new IOException("The provided path isn't a directory");
    }
    FileStatus[] files = fs.listStatus(path, nonHiddenPathFilter);
    for (FileStatus file : files) {
      paths.add(new Path(file.getPath().toUri().getPath()));
    }
    return paths;
  }

  /**
   * Returns the path string for each path in the given list.
   * 
   * @param paths list of paths
   * @return list of path strings
   */
  public static List<String> getPathsToString(List<Path> paths) {
    List<String> pathStrings = new ArrayList<String>();
    for (Path p : paths) {
      pathStrings.add(p.toString());
    }
    return pathStrings;
  }

  /**
   * Gets the date for a path in the specified format.
   *
   * @param path
   *          path to check
   * @param dateFormat the date format
   * @return date the date
   */
  public static Date getDateForPath(Path path, SimpleDateFormat dateFormat) {
    Pattern pattern = Pattern.compile(".+/(.{" + dateFormat.toPattern().length() + "})");
    Matcher matcher = pattern.matcher(path.toString());

    if (!matcher.matches()) {
      throw new RuntimeException("Unexpected input filename: " + path);
    }

    try {
      return dateFormat.parse(matcher.group(1));
    } catch (ParseException e) {
      throw new RuntimeException("Unexpected input filename: " + path);
    }
  }

  /**
   * Gets the root path for a path in the provided date format. This is part of the path preceding
   * the date pattern (e.g. "yyyy/MM/dd") portion.
   *
   * @param path
   *          path to get root
   * @param dateFormat the date format
   * @return root path
   */
  public static Path getRootForDatePath(Path path, SimpleDateFormat dateFormat) {
    Pattern pattern = Pattern.compile(".+/(.{" + dateFormat.toPattern().length() + "})");
    Matcher matcher = pattern.matcher(path.toString());

    if (!matcher.matches()) {
      throw new RuntimeException("Unexpected input filename: " + path);
    }

    return new Path(path.toString().substring(0, path.toString().length() - dateFormat.toPattern().length()));
  }

  /**
   * Returns the index of the given path in the list of input paths.
   *
   * @param context
   *          job context
   * @param path
   *          path to get index
   * @return path index
   */
  public static int getInputPathIndex(JobContext context, Path path) {
    Path[] paths = FileInputFormat.getInputPaths(context);
    int i = 0;
    for (Path p : paths) {
      if (path.equals(p)) {
        return i;
      }
      i++;
    }
    return -1;
  }
}
