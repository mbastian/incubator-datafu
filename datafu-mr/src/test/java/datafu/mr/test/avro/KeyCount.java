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

package datafu.mr.test.avro;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class KeyCount extends org.apache.avro.specific.SpecificRecordBase implements
    org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ =
      new org.apache.avro.Schema.Parser()
          .parse("{\"type\":\"record\",\"name\":\"KeyCount\",\"namespace\":\"datafu.mr.test.avro\",\"fields\":[{\"name\":\"key\",\"type\":\"long\"},{\"name\":\"count\",\"type\":\"long\"}]}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }
  @Deprecated
  public long key;
  @Deprecated
  public long count;

  /**
   * Default constructor.
   */
  public KeyCount() {
  }

  /**
   * All-args constructor.
   */
  public KeyCount(java.lang.Long key, java.lang.Long count) {
    this.key = key;
    this.count = count;
  }

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
      case 0:
        return key;
      case 1:
        return count;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
      case 0:
        key = (java.lang.Long) value$;
        break;
      case 1:
        count = (java.lang.Long) value$;
        break;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'key' field.
   */
  public java.lang.Long getKey() {
    return key;
  }

  /**
   * Sets the value of the 'key' field.
   * @param value the value to set.
   */
  public void setKey(java.lang.Long value) {
    this.key = value;
  }

  /**
   * Gets the value of the 'count' field.
   */
  public java.lang.Long getCount() {
    return count;
  }

  /**
   * Sets the value of the 'count' field.
   * @param value the value to set.
   */
  public void setCount(java.lang.Long value) {
    this.count = value;
  }

  /** Creates a new KeyCount RecordBuilder */
  public static datafu.mr.test.avro.KeyCount.Builder newBuilder() {
    return new datafu.mr.test.avro.KeyCount.Builder();
  }

  /** Creates a new KeyCount RecordBuilder by copying an existing Builder */
  public static datafu.mr.test.avro.KeyCount.Builder newBuilder(datafu.mr.test.avro.KeyCount.Builder other) {
    return new datafu.mr.test.avro.KeyCount.Builder(other);
  }

  /** Creates a new KeyCount RecordBuilder by copying an existing KeyCount instance */
  public static datafu.mr.test.avro.KeyCount.Builder newBuilder(datafu.mr.test.avro.KeyCount other) {
    return new datafu.mr.test.avro.KeyCount.Builder(other);
  }

  /**
   * RecordBuilder for KeyCount instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<KeyCount> implements
      org.apache.avro.data.RecordBuilder<KeyCount> {

    private long key;
    private long count;

    /** Creates a new Builder */
    private Builder() {
      super(datafu.mr.test.avro.KeyCount.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(datafu.mr.test.avro.KeyCount.Builder other) {
      super(other);
    }

    /** Creates a Builder by copying an existing KeyCount instance */
    private Builder(datafu.mr.test.avro.KeyCount other) {
      super(datafu.mr.test.avro.KeyCount.SCHEMA$);
      if (isValidValue(fields()[0], other.key)) {
        this.key = data().deepCopy(fields()[0].schema(), other.key);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.count)) {
        this.count = data().deepCopy(fields()[1].schema(), other.count);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'key' field */
    public java.lang.Long getKey() {
      return key;
    }

    /** Sets the value of the 'key' field */
    public datafu.mr.test.avro.KeyCount.Builder setKey(long value) {
      validate(fields()[0], value);
      this.key = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'key' field has been set */
    public boolean hasKey() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'key' field */
    public datafu.mr.test.avro.KeyCount.Builder clearKey() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'count' field */
    public java.lang.Long getCount() {
      return count;
    }

    /** Sets the value of the 'count' field */
    public datafu.mr.test.avro.KeyCount.Builder setCount(long value) {
      validate(fields()[1], value);
      this.count = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /** Checks whether the 'count' field has been set */
    public boolean hasCount() {
      return fieldSetFlags()[1];
    }

    /** Clears the value of the 'count' field */
    public datafu.mr.test.avro.KeyCount.Builder clearCount() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public KeyCount build() {
      try {
        KeyCount record = new KeyCount();
        record.key = fieldSetFlags()[0] ? this.key : (java.lang.Long) defaultValue(fields()[0]);
        record.count = fieldSetFlags()[1] ? this.count : (java.lang.Long) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
