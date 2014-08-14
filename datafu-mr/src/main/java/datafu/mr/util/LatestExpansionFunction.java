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

package datafu.mr.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datafu.mr.fs.DatePath;
import datafu.mr.fs.PathUtils;


/**
 * Utility to obtain the latest path from a folder
 *
 * @author Matthew Hayes
 */
public class LatestExpansionFunction {
  private final SimpleDateFormat dateFormat;
  private final Logger log;
  private final String latestSuffix;
  private final FileSystem fs;
  private final boolean matchDate;

  /**
   * Creates the function based on a custom suffix.
   *
   * @param fs file system
   * @param latestSuffix the latest suffix
   * @param dateFormat date format
   * @param matchDate whether to match the date
   * @param log logger
   */
  public LatestExpansionFunction(FileSystem fs, String latestSuffix, SimpleDateFormat dateFormat, boolean matchDate,
      Logger log) {
    this.log = log;
    this.fs = fs;
    this.dateFormat = dateFormat;
    this.matchDate = matchDate;
    this.latestSuffix = latestSuffix;
  }

  /**
   * Creates the function based the default '#LATEST' suffix.
   *
   * @param fs file system
   * @param log logger
   */
  public LatestExpansionFunction(FileSystem fs, SimpleDateFormat dateFormat, boolean matchDate, Logger log) {
    this(fs, "#LATEST", dateFormat, matchDate, log);
  }

  /**
   * Returns the latest path.
   *
   * @param path
   *          path which contains the latest suffix
   * @return the expanded path
   */
  public String apply(String path) {
    if (path.contains(latestSuffix)) {
      String actualPath = path.substring(0, path.indexOf(latestSuffix));
      String suffix = path.substring(path.indexOf(latestSuffix)).replaceAll(latestSuffix, "");

      try {
        Path rootPath = new Path(actualPath);
        if (!fs.exists(rootPath)) {
          throw new RuntimeException("The path " + rootPath + " can't be found");
        }

        if (matchDate) {
          List<DatePath> childPaths;
          if (dateFormat.toPattern().contains("/")) {
            childPaths = PathUtils.findNestedDatedPaths(fs, rootPath, dateFormat);
          } else {
            childPaths = PathUtils.findDatedPaths(fs, rootPath, dateFormat);
          }

          if (childPaths.isEmpty()) {
            throw new RuntimeException(String.format("No files found under path[%s] when resolving path[%s]. fs[%s]",
                actualPath, path, fs));
          }
          return childPaths.get(childPaths.size() - 1).getPath().toUri().getPath() + suffix;
        } else {
          FileStatus[] files = fs.listStatus(new Path(actualPath));

          List<FileStatus> filtered = new ArrayList<FileStatus>();
          for (FileStatus fs : files) {
            String name = fs.getPath().getName();
            if (!name.startsWith("_") && !name.startsWith(".")) {
              filtered.add(fs);
            }
          }
          files = filtered.toArray(new FileStatus[0]);

          if (files.length == 0) {
            throw new RuntimeException(String.format("No files found under path[%s] when resolving path[%s]. fs[%s]",
                actualPath, path, fs));
          }

          Arrays.sort(files, new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus o1, FileStatus o2) {
              return o1.getPath().getName().compareTo(o2.getPath().getName());
            }
          });
          return files[files.length - 1].getPath().toUri().getPath() + suffix;
        }
      } catch (IOException e) {
        final String message = String.format("Exception when looking for expansion of %s", latestSuffix);

        log.error(message, e);
        throw new RuntimeException(message, e);
      }
    }
    return path;
  }

  /**
   * Return the suffix.
   *
   * @return latest suffix
   */
  public String getLatestSuffix() {
    return latestSuffix;
  }
}
