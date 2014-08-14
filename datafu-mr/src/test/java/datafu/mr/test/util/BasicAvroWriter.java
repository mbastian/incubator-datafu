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
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Utility to write Avro records to a specific location.
 *
 * @param <T> the type of the records
 */
public class BasicAvroWriter<T> {

  protected final Path outputPath;
  protected final Schema schema;
  protected final FileSystem fs;

  protected Path outputFilePath;
  private DataFileWriter<Object> dataWriter;
  private OutputStream outputStream;

  /**
   * Creates the writer at the given location with the provided schema.
   *
   * @param outputPath the path to write to
   * @param schema the record schema
   * @param fs the file system
   */
  public BasicAvroWriter(Path outputPath, Schema schema, FileSystem fs) {
    this.outputPath = outputPath;
    this.schema = schema;
    this.fs = fs;
    this.outputFilePath = new Path(outputPath, "part-00000.avro");
  }

  /**
   * Opens the writer.
   * <p>
   * Will throw an error if opened multiple times
   *
   * @throws IOException if an error occurs
   */
  public void open() throws IOException {
    if (dataWriter != null) {
      throw new RuntimeException("Already have data writer");
    }

    outputStream = fs.create(outputFilePath);

    GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>();
    dataWriter = new DataFileWriter<Object>(writer);
    dataWriter.create(schema, outputStream);
  }

  /**
   * Appends a record to the writer.
   * <p>
   * The writer has to be opened
   *
   * @param record the record to append
   * @throws IOException if an error occurs
   */
  public void append(T record) throws IOException {
    if (dataWriter == null) {
      throw new RuntimeException("No data writer");
    }
    dataWriter.append(record);
  }

  /**
   * Construct and appends a record to the writer based on the writer's schema.
   * <p>
   * The length and order of parameters must match the schema.
   *
   * @param fieldValues the values of each field
   * @throws IOException if an error occurs
   * @throws IllegalArgumentException if the number of values differs from the schema
   */
  public void append(Object... fieldValues) throws IOException {
    if (dataWriter == null) {
      throw new RuntimeException("No data writer");
    }
    if (fieldValues.length != schema.getFields().size()) {
      throw new IllegalArgumentException(String.format("The schema has %s fields but %s values were provided", schema
          .getFields().size(), fieldValues.length));
    }
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      record.put(f.name(), fieldValues[f.pos()]);
    }

    dataWriter.append(record);
  }

  /**
   * Closes the writer.
   * <p>
   * Will throw an error if closed multiple times
   *
   * @throws IOException if an error occurs
   */
  public void close() throws IOException {
    if (dataWriter == null) {
      throw new RuntimeException("No data writer");
    }
    dataWriter.close();
    outputStream.close();
    dataWriter = null;
    outputStream = null;
  }

  /**
   * Writes all records.
   * <p>
   * The writer is being closed when all records have been written
   *
   * @param records the records to write
   * @throws IOException if an error occurs
   */
  public void writeAll(T... records) throws IOException {
    open();
    try {
      for (T record : records) {
        dataWriter.append(record);
      }
    } finally {
      close();
    }
  }

  /**
   * Returns the output file path.
   *
   * @return output file path
   */
  public Path getOutputFilePath() {
    return outputFilePath;
  }
}
