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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


/**
 * Utility to support various Avro features.
 *
 * @author Mathieu Bastian
 */
public class AvroHelper {

  private static final Logger _log = Logger.getLogger(AvroHelper.class);

  /**
   * Returns the schema associated with the provided class.
   * <p>
   * If the class is an Avro <em>SpecificRecord</em>, it will return the record's schema. If the class is a POJO object, it
   * will infer the schema using reflection.
   * <p>
   * It throws an <em>IllegalArgumentException</em> exception if the provided class is a <em>GenericRecord</em>.
   *
   * @param avroClass the class to get the schema from
   * @return the avro schema
   */
  public static Schema getSchema(Class<?> avroClass) {
    Schema schema;
    if (SpecificRecord.class.isAssignableFrom(avroClass)) {
      try {
        schema = (Schema) avroClass.getDeclaredField("SCHEMA$").get(null);
        _log.info(String.format("Infer schema from specific record %s class: %s", avroClass.getName(),
            schema.toString()));
      } catch (Exception exc) {
        throw new RuntimeException(String.format(
            "The record class %s should have a static `SCHEMA$` field that returns the record's Avro schema.",
            avroClass.getName()), exc);
      }
    } else if (GenericRecord.class.isAssignableFrom(avroClass)) {
      _log.warn(String.format("Can't infer schema of GenericRecord (class %s)", avroClass.getName()));
      throw new IllegalArgumentException(String.format("A schema needs to be provided for the %s class",
          avroClass.getName()));
    } else {
      schema = ReflectData.get().getSchema(avroClass);
      _log.info(String.format("Infer schema from %s class: %s", avroClass.getName(), schema.toString()));
    }
    return schema;
  }

  /**
   * Read the given file or folder recursively to obtain all Avro records.
   * <p>
   * This function can either return <em>GenericRecord</em> or <em>SpecificRecord</em>. If <em>specificRecordClass</em>
   * is null it will return generic records.
   * 
   * @param fileOrFolder file or folder to read
   * @param specificRecordClass specific record class or null
   * @return iterable over all records contained in <em>fileOrFolder</em>
   */
  public static <T> Iterable<T> readAvroFiles(File fileOrFolder, Class<T> specificRecordClass) {
    if (specificRecordClass != null && !SpecificRecord.class.isAssignableFrom(specificRecordClass)) {
      throw new IllegalArgumentException(String.format("The specific record class %s should implement SpecificRecord",
          specificRecordClass.getName()));
    }
    if (!fileOrFolder.exists()) {
      _log.info(String.format("The %s folder doesn't exist, returning an empty iterable", fileOrFolder.getPath()));
      return new AvroIterable<T>(Arrays.asList(new File[0]), specificRecordClass);
    }
    Collection<File> allFiles;
    if (fileOrFolder.isDirectory()) {
      allFiles = FileUtils.listFiles(fileOrFolder, new String[] { "avro" }, true);
      _log.info(String.format("Found %d files in the %s folder", allFiles.size(), fileOrFolder.getPath()));
    } else {
      allFiles = Arrays.asList(new File[] { fileOrFolder });
    }
    return new AvroIterable<T>(allFiles, specificRecordClass);
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getMapperInputAvroKeyTypeClass(Class mapperClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(mapperClass, Mapper.class)[0];
    Class<?> keyClass = (Class<?>) type.getActualTypeArguments()[0];
    return keyClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getMapperInputAvroValueTypeClass(Class mapperClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(mapperClass, Mapper.class)[1];
    Class<?> valueClass = (Class<?>) type.getActualTypeArguments()[0];
    return valueClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getMapperOutputAvroKeyTypeClass(Class mapperClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(mapperClass, Mapper.class)[2];
    Class<?> keyClass = (Class<?>) type.getActualTypeArguments()[0];
    return keyClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getMapperOutputAvroValueTypeClass(Class mapperClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(mapperClass, Mapper.class)[3];
    Class<?> valueClass = (Class<?>) type.getActualTypeArguments()[0];
    return valueClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getReducerInputAvroKeyTypeClass(Class reducerClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(reducerClass, Reducer.class)[0];
    Class<?> keyClass = (Class<?>) type.getActualTypeArguments()[0];
    return keyClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getReducerInputAvroValueTypeClass(Class reducerClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(reducerClass, Reducer.class)[1];
    Class<?> valueClass = (Class<?>) type.getActualTypeArguments()[0];
    return valueClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getReducerOutputAvroKeyTypeClass(Class reducerClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(reducerClass, Reducer.class)[2];
    Class<?> keyClass = (Class<?>) type.getActualTypeArguments()[0];
    return keyClass;
  }

  @SuppressWarnings("rawtypes")
  public static Class<?> getReducerOutputAvroValueTypeClass(Class reducerClass) {
    ParameterizedType type = (ParameterizedType) IntermediateTypeHelper.getTypes(reducerClass, Reducer.class)[3];
    Class<?> valueClass = (Class<?>) type.getActualTypeArguments()[0];
    return valueClass;
  }

  /**
   * Avro record iterable over multiple files.
   * <p>
   * The iterator will return <em>GenericRecord</em> if the constructor's <em>specificRecordClass</em> is null.
   *
   * @param <E> the Avro record class if <em>SpecificRecord</em> or <em>GenericRecord</em> otherwise
   */
  private static class AvroIterable<E> implements Iterable<E> {

    private final Collection<File> files;
    private final Class<E> specificRecordClass;

    public AvroIterable(Collection<File> files, Class<E> specificRecord) {
      this.files = files;
      this.specificRecordClass = specificRecord;
    }

    @Override
    public Iterator<E> iterator() {
      return new AvroIterator<E>(files, specificRecordClass);
    }
  }

  /**
   * Avro record iterator over multiple files.
   * <p>
   * The iterator will return <em>GenericRecord</em> if the constructor's <em>specificRecordClass</em> is null.
   *
   * @param <E> the Avro record class if <em>SpecificRecord</em> or <em>GenericRecord</em> otherwise
   */
  private static class AvroIterator<E> implements Iterator<E> {

    private final Iterator<File> files;
    private final Class<E> specificRecordClass;
    private DataFileStream<E> dataStream;
    private InputStream fileInputStream;

    public AvroIterator(Collection<File> files, Class<E> specificRecordClass) {
      this.files = files.iterator();
      this.specificRecordClass = specificRecordClass;
    }

    @Override
    public boolean hasNext() {
      try {
        while (dataStream == null || !dataStream.hasNext()) {
          if (!files.hasNext()) {
            return false;
          }
          if (dataStream != null) {
            try {
              dataStream.close();
              fileInputStream.close();
            } catch (IOException e) {
              throw new RuntimeException("Error while closing avro files", e);
            }
          }
          DatumReader<E> reader =
              specificRecordClass != null ? new SpecificDatumReader<E>(specificRecordClass)
                  : new GenericDatumReader<E>();
          fileInputStream = new BufferedInputStream(new FileInputStream(files.next()));
          dataStream = new DataFileStream<E>(fileInputStream, reader);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error while reading avro files", e);
      }
      return dataStream.hasNext();
    }

    @Override
    public E next() {
      E res = dataStream.next();
      if (!hasNext()) {
        try {
          dataStream.close();
          fileInputStream.close();
        } catch (IOException e) {
          throw new RuntimeException("Error while closing avro files", e);
        }
      }
      return res;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }
  }
}
