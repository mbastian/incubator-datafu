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

package datafu.mr.test.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Utility to read Avro records from a specific location.
 *
 * @param <T> the type of the records
 */
public class BasicAvroReader<T> {

  private final Path inputPath;
  private final FileSystem fs;
  private final Class<T> recordClass;

  private DataFileStream<T> dataReader;

  /**
   * Creates the reader from the specific location.
   *
   * @param inputPath the path to read from
   * @param fs the file system
   */
  public BasicAvroReader(Path inputPath, FileSystem fs) {
    this.inputPath = inputPath;
    this.fs = fs;
    this.recordClass = null;
  }

  /**
   * Creates the reader from the specific location and class.
   *
   * @param inputPath the path to read from
   * @param fs the file system
   * @param specificRecordClass class which implements specific record
   */
  public BasicAvroReader(Path inputPath, FileSystem fs, Class<T> specificRecordClass) {
    this.inputPath = inputPath;
    this.fs = fs;
    this.recordClass = specificRecordClass;
  }

  /**
   * Opens the reader.
   * <p>
   * Will throw an exception if opened multiple times
   *
   * @throws IOException if an error occurs
   */
  private void open() throws IOException {
    if (dataReader != null) {
      throw new RuntimeException("Already have data reader");
    }

    Path path = new Path(inputPath, "part-r-00000.avro");

    if (!fs.exists(path)) {
      path = new Path(inputPath, "part-m-00000.avro");
    }

    FSDataInputStream is = fs.open(path);
    DatumReader<T> reader = recordClass == null ? new GenericDatumReader<T>() : new SpecificDatumReader<T>(recordClass);
    dataReader = new DataFileStream<T>(is, reader);
  }

  /**
   * Reads the entire file and returns a list of entries.
   *
   * @return the entries the file contains
   * @throws IOException if an error occurs
   */
  public List<T> readAll() throws IOException {
    open();
    try {
      List<T> res = new ArrayList<T>();
      while (dataReader.hasNext()) {
        T r = (T) dataReader.next();
        res.add(r);
      }
      return res;
    } finally {
      close();
    }
  }

  /**
   * Reads the file and returns the first entry.
   *
   * @return the first entry in the file
   * @throws IOException if an error occurs
   */
  public T readFirst() throws IOException {
    open();
    try {
      if (dataReader.hasNext()) {
        T r = (T) dataReader.next();
        return r;
      }
      return null;
    } finally {
      close();
    }
  }

  /**
   * Reads the entire file and returns a map of entries where the key is found in the provided field.
   * @param fieldName the field where the map key is to be found
   * @param <K> the map key type
   * @return the map of entries the file contains
   * @throws IOException if an error occurs
   */
  public <K> Map<K, T> readAndMapAll(String fieldName) throws IOException {
    open();
    try {
      Map<K, T> res = new LinkedHashMap<K, T>();
      while (dataReader.hasNext()) {
        T r = (T) dataReader.next();
        GenericRecord record = (GenericRecord) r;
        if (record.getSchema().getField(fieldName) == null) {
          throw new IllegalArgumentException(String.format("The schema doesn't contain the %s field", fieldName));
        }
        @SuppressWarnings("unchecked")
        K val = (K) record.get(fieldName);
        if (val == null) {
          throw new RuntimeException(String.format("The value for the %s field is null", fieldName));
        }
        if (res.containsKey(val)) {
          throw new RuntimeException(String.format("The file contains duplicates for the %s value", val));
        }
        res.put(val, r);
      }
      return res;
    } finally {
      close();
    }
  }

  /**
   * Closes the reader.
   * <p>
   * Will throw an exception if closed multiple times
   *
   * @throws IOException if an error occurs
   */
  private void close() throws IOException {
    if (dataReader == null) {
      throw new RuntimeException("No data reader");
    }
    dataReader.close();
    dataReader = null;
  }
}
