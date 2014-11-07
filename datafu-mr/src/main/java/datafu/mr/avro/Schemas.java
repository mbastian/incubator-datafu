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

package datafu.mr.avro;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;


/**
 * Avro schema utilities
 *
 * @author Mathieu Bastian
 */
public class Schemas {
  /**
   * Creates a record schema with name and package derived from a class and with the provided fields
   *
   * @param cls
   *          class
   * @param name
   *          schema name
   * @param fields
   *          list of fields
   * @return Avro record schema
   */
  public static Schema createRecordSchema(@SuppressWarnings("rawtypes") Class cls, String name, Field... fields) {
    Schema record = Schema.createRecord(cls.getName() + name, null, cls.getPackage().getName(), false);

    if (fields.length > 0) {
      record.setFields(Arrays.asList(fields));
    }

    return record;
  }

  /**
   * Creates a record schema with name and package derived from a class. "Key" is appended to the
   * class name to derive the record's name.
   *
   * @param cls
   *          class
   * @return Avro record schema
   */
  public static Schema createKeyRecordSchema(@SuppressWarnings("rawtypes") Class cls) {
    return createRecordSchema(cls, "Key");
  }

  /**
   * Creates a record schema with name and package derived from a class. "Key" is appended to the
   * class name to derive the record's name.
   * 
   * @param cls
   *          class
   * @param fields
   *          list of fields
   * @return Avro record schema
   */
  public static Schema createKeyRecordSchema(@SuppressWarnings("rawtypes") Class cls, Field... fields) {
    return createRecordSchema(cls, "Key", fields);
  }
}
