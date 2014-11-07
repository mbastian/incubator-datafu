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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


/**
 * A combined input format for reading Avro data.
 *
 * @author Matthew Hayes
 *
 * @param <T>
 *          Type of data to be read
 */
public class CombinedAvroMultipleInputsKeyInputFormat<T> extends CombineFileInputFormat<AvroKey<T>, NullWritable> {
  private final Logger LOG = Logger.getLogger(CombinedAvroMultipleInputsKeyInputFormat.class);

  public static class CombinedAvroKeyRecordReader<T> extends AvroKeyRecordReader<T> {
    private CombineFileSplit inputSplit;
    private Integer idx;

    public CombinedAvroKeyRecordReader(CombineFileSplit inputSplit, TaskAttemptContext context, Integer idx) {
      super(AvroJob.getInputKeySchema(context.getConfiguration()));
      this.inputSplit = inputSplit;
      this.idx = idx;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      this.inputSplit = (CombineFileSplit) inputSplit;

      FileSplit fileSplit =
          new FileSplit(this.inputSplit.getPath(idx), this.inputSplit.getOffset(idx), this.inputSplit.getLength(idx),
              this.inputSplit.getLocations());

      super.initialize(fileSplit, context);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException {
    Schema readerSchema = AvroMultipleInputsUtil.getInputKeySchemaForSplit(context.getConfiguration(), inputSplit);
    if (null == readerSchema) {
      throw new RuntimeException("Could not determine input schema");
    }

    Object c = CombinedAvroKeyRecordReader.class;
    return new CombineFileRecordReader<AvroKey<T>, NullWritable>((CombineFileSplit) inputSplit, context,
        (Class<? extends RecordReader<AvroKey<T>, NullWritable>>) c);
  }

}
