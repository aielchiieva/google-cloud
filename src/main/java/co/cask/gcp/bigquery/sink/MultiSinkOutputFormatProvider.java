/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.gcp.bigquery.sink;

import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.bigquery.util.BigQueryUtil;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * Provides {@link MultiSinkOutputFormatDelegate} to output values for multiple tables.
 */
public class MultiSinkOutputFormatProvider implements OutputFormatProvider {

  private static final String FILTER_FIELD = "bq.multi.record.filter.field";
  private static final String FILTER_VALUE = "bq.multi.record.filter.value";
  private static final String SCHEMA = "bq.multi.record.schema";

  private final Configuration config;

  public MultiSinkOutputFormatProvider(Configuration config,
                                       String tableName,
                                       Schema tableSchema,
                                       String filterField) {
    this.config = new Configuration(config);
    this.config.set(FILTER_VALUE, tableName);
    this.config.set(FILTER_FIELD, filterField);
    this.config.set(SCHEMA, tableSchema.toString());
  }

  @Override
  public String getOutputFormatClassName() {
    return MultiSinkOutputFormatDelegate.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return BigQueryUtil.configToMap(config);
  }

  /**
   * Uses {@link IndirectBigQueryOutputFormat} as delegate and creates {@link FilterRecordWriter}
   * to output values based on filter and its value and schema.
   */
  public static class MultiSinkOutputFormatDelegate extends OutputFormat<JsonObject, NullWritable> {

    private final OutputFormat delegate;

    public MultiSinkOutputFormatDelegate() {
      this.delegate = new IndirectBigQueryOutputFormat();
    }

    @Override
    public RecordWriter<JsonObject, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      Configuration conf = taskAttemptContext.getConfiguration();
      String filterField = conf.get(FILTER_FIELD);
      String filterValue = conf.get(FILTER_VALUE);
      Schema schema = Schema.parseJson(conf.get(SCHEMA));
      @SuppressWarnings("unchecked")
      RecordWriter<JsonObject, NullWritable> recordWriter = delegate.getRecordWriter(taskAttemptContext);
      return new FilterRecordWriter(filterField, filterValue, schema, recordWriter);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
      delegate.checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      return delegate.getOutputCommitter(taskAttemptContext);
    }
  }

  /**
   * Filters records before writing them out using a delegate based on filter and its value and given schema.
   */
  public static class FilterRecordWriter extends RecordWriter<JsonObject, NullWritable> {

    private final String filterField;
    private final String filterValue;
    private final Schema schema;
    private final RecordWriter<JsonObject, NullWritable> delegate;


    public FilterRecordWriter(String filterField,
                              String filterValue,
                              Schema schema,
                              RecordWriter<JsonObject, NullWritable> delegate) {
      this.filterField = filterField;
      this.filterValue = filterValue;
      this.schema = schema;
      this.delegate = delegate;
    }

    @Override
    public void write(JsonObject key, NullWritable value) throws IOException, InterruptedException {
      JsonElement jsonElement = key.get(filterField);
      if (jsonElement == null || !filterValue.equalsIgnoreCase(jsonElement.getAsString())) {
        return;
      }

      JsonObject object = new JsonObject();
      key.entrySet().stream()
        .filter(entry -> !filterField.equals(entry.getKey())) // exclude filter field
        .filter(entry -> schema.getField(entry.getKey()) != null) // exclude fields which are not in schema
        .forEach(entry -> object.add(entry.getKey(), entry.getValue()));

      delegate.write(object, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.close(context);
    }
  }

}
