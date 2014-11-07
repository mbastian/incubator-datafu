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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import datafu.mr.avro.CombinedAvroKeyInputFormat;
import datafu.mr.fs.PathUtils;
import datafu.mr.util.AvroHelper;
import datafu.mr.util.DiscoveryHelper;
import datafu.mr.util.IntermediateTypeHelper;
import datafu.mr.util.LatestExpansionFunction;


/**
 * Base class for Avro MapReduce jobs.
 *
 * <p>
 * This class extends <em>AbstractJob</em> and configures the input/output to be Avro. Behind the
 * scenes it uses a <em>AvroKeyInputFormat</em> so the expected map input key class is
 * <em>AvroKey</em> and the map input value class is <em>NullWritable</em>.
 * </p>
 *
 * This class recognizes the following properties:
 *
 * <ul>
 * <li><em>combine.inputs</em> - Combine input paths (boolean)</li>
 * <li><em>map.output.key.schema</em> - Map output key Avro schema</li>
 * <li><em>map.output.value.schema</em> - Map output value Avro schema</li>
 * <li><em>output.schema</em> - Output Avro schema</li>
 * </ul>
 *
 * <p>
 * When using Avro's <em>GenericRecord</em> to pass data from the mapper to the reducer, implement
 * the <em>getMapOutputKeySchema()</em> and <em>getMapOutputValueSchema()</em> methods to specify
 * the schemas. If the types are POJO, their schema will be inferred if not specified.
 * </p>
 *
 * <p>
 * If the output type is an Avro <em>GenericRecord</em>, implement the <em>getOutputSchema()</em>
 * method to specify the schema. If the type is POJO, its schema will be inferred if not specified.
 * </p>
 *
 * @author Mathieu Bastian
 */
public abstract class AbstractAvroJob extends AbstractJob {
  private final Logger _log = Logger.getLogger(AbstractAvroJob.class);

  protected boolean combineInputs;
  protected Schema outputSchema;
  protected Schema mapOutputKeySchema;
  protected Schema mapOutputValueSchema;

  public AbstractAvroJob() {
    super();
  }

  public AbstractAvroJob(String name, Properties props) {
    super(name, props);
  }

  @Override
  public void setProperties(Properties props) {
    super.setProperties(props);

    if (props.containsKey("combine.inputs")) {
      setCombineInputs(Boolean.parseBoolean(props.getProperty("combine.inputs")));
    }
    if (props.containsKey("map.output.key.schema")) {
      setMapOutputKeySchema(new Schema.Parser().parse(props.getProperty("map.output.key.schema")));
    }
    if (props.containsKey("map.output.value.schema")) {
      setMapOutputValueSchema(new Schema.Parser().parse(props.getProperty("map.output.value.schema")));
    }
    if (props.containsKey("output.schema")) {
      setOutputSchema(new Schema.Parser().parse(props.getProperty("output.schema")));
    }
  }

  /**
   * Gets the Avro output schema.
   *
   * @return output schema
   */
  public Schema getOutputSchema() {
    return outputSchema;
  }

  /**
   * Sets the Avro output schema
   *
   * @param schema
   *          output schema
   */
  public void setOutputSchema(Schema schema) {
    this.outputSchema = schema;
  }

  /**
   * Gets the Avro map output key schema.
   *
   * @return map key schema
   */
  public Schema getMapOutputKeySchema() {
    return mapOutputKeySchema;
  }

  /**
   * Sets the Avro map output key schema
   *
   * @param schema
   *          map key schema
   */
  public void setMapOutputKeySchema(Schema schema) {
    this.mapOutputKeySchema = schema;
  }

  /**
   * Gets the Avro map output value schema
   *
   * @return map value schema
   */
  public Schema getMapOutputValueSchema() {
    return mapOutputValueSchema;
  }

  /**
   * Sets the Avro map output value schema
   *
   * @param schema
   *          map value schema
   */
  public void setMapOutputValueSchema(Schema schema) {
    this.mapOutputValueSchema = schema;
  }

  @Override
  public void setupInputFormat(Job job) throws IOException {
    if (getCombineInputs()) {
      job.setInputFormatClass(CombinedAvroKeyInputFormat.class);
      _log.info(String.format("Set input format class: %s", CombinedAvroKeyInputFormat.class.getSimpleName()));
    } else {
      job.setInputFormatClass(AvroKeyInputFormat.class);
      _log.info(String.format("Set input format class: %s", AvroKeyInputFormat.class.getSimpleName()));
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void configure(Job job) {
    super.configure(job);
    Class<? extends Mapper> mapperClass = DiscoveryHelper.getMapperClass(this);
    Class<? extends Reducer> reducerClass = DiscoveryHelper.getReducerClass(this);

    if (reducerClass != null) {
      if (AvroKey.class.isAssignableFrom(IntermediateTypeHelper.getMapperOutputKeyClass(mapperClass))) {
        if (getMapOutputKeySchema() != null) {
          AvroJob.setMapOutputKeySchema(job, getMapOutputKeySchema());
          _log.info(String.format("Set map output key schema: %s", getMapOutputKeySchema().toString()));
        } else {
          Class<?> keyClass = AvroHelper.getMapperOutputAvroKeyTypeClass(mapperClass);
          Schema keySchema = AvroHelper.getSchema(keyClass);
          AvroJob.setMapOutputKeySchema(job, keySchema);
          _log.info(String.format("Set map output key schema: %s", keySchema.toString()));
        }
      }
      if (AvroValue.class.isAssignableFrom(IntermediateTypeHelper.getMapperOutputValueClass(mapperClass))) {
        if (getMapOutputValueSchema() != null) {
          AvroJob.setMapOutputValueSchema(job, getMapOutputValueSchema());
          _log.info(String.format("Set map output value schema: %s", getMapOutputValueSchema().toString()));
        } else {
          Class<?> valueClass = AvroHelper.getMapperOutputAvroValueTypeClass(mapperClass);
          Schema valueSchema = AvroHelper.getSchema(valueClass);
          AvroJob.setMapOutputValueSchema(job, valueSchema);
          _log.info(String.format("Set map output value schema: %s", valueSchema.toString()));
        }
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void setupOutputFormat(Job job) throws IOException {
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Schema outputSchema = getOutputSchema();
    if (outputSchema != null) {
      AvroJob.setOutputKeySchema(job, getOutputSchema());
      _log.info(String.format("Set output key schema: %s", getOutputSchema().toString()));
    } else {
      Class<?> keyClass;
      Class<? extends Reducer> reducerClass = DiscoveryHelper.getReducerClass(this);
      if (reducerClass == null) {
        keyClass = AvroHelper.getMapperOutputAvroKeyTypeClass(DiscoveryHelper.getMapperClass(this));
      } else {
        keyClass = AvroHelper.getReducerOutputAvroKeyTypeClass(reducerClass);
      }
      Schema keySchema = AvroHelper.getSchema(keyClass);
      AvroJob.setOutputKeySchema(job, keySchema);
      _log.info(String.format("Set output key schema: %s", keySchema.toString()));
    }
  }

  /**
   * Gets whether inputs should be combined.
   *
   * @return true if inputs are to be combined
   */
  public boolean getCombineInputs() {
    return combineInputs;
  }

  /**
   * Sets whether inputs should be combined.
   *
   * @param combineInputs
   *          true to combine inputs
   */
  public void setCombineInputs(boolean combineInputs) {
    this.combineInputs = combineInputs;
  }

  /**
   * Returns the set of input schema, one schema per input path
   *
   * @return the set of input schema
   * @throws IOException IOException
   */
  protected Map<String, Schema> getInputSchemas() throws IOException {
    Map<String, Schema> schemas = new LinkedHashMap<String, Schema>();
    LatestExpansionFunction latestExpansionFunction =
        new LatestExpansionFunction(getFileSystem(), getLatestExpansionDateFormat(), getUseLatestExpansion().equals(
            "date"), _log);
    for (Path p : getInputPaths()) {
      p = new Path(isUseLatestExpansion() ? latestExpansionFunction.apply(p.toString()) : p.toString());
      Schema schema = PathUtils.getSchemaFromPath(getFileSystem(), p);
      String pathName = p.getName();
      _log.info(String.format("Got schema from path: %s\n%s", pathName, schema.toString()));
      schemas.put(pathName, schema);
    }
    return schemas;
  }
}
