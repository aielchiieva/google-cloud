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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This plugin allows users that would to write {@link StructuredRecord} entries
 * to multiple Google Big Query tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("BigQueryMultiTable")
@Description("Writes records to one or more Big Query tables. " +
  "BigQuery is Google's serverless, highly scalable, enterprise data warehouse. " +
  "Data is first written to a temporary location on Google Cloud Storage, then loaded into BigQuery from there.")
public class BigQueryMultiSink extends AbstractBigQuerySink {

  private static final String TABLE_PREFIX = "multisink.";

  private final BigQueryMultiSinkConfig config;

  public BigQueryMultiSink(BigQueryMultiSinkConfig config) {
    this.config = config;
  }

  @Override
  protected BigQueryMultiSinkConfig getConfig() {
    return config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<JsonObject, NullWritable>> emitter) {
    JsonObject object = new JsonObject();
    for (Schema.Field recordField : Objects.requireNonNull(input.getSchema().getFields())) {
      decodeSimpleTypes(object, recordField.getName(), input);
    }
    emitter.emit(new KeyValue<>(object, NullWritable.get()));
  }

  @Override
  protected void prepareRunValidation(BatchSinkContext context) {
    // no-op
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, String bucket) throws IOException {
    Map<String, String> arguments = new HashMap<>(context.getArguments().asMap());
    for (Map.Entry<String, String> argument : arguments.entrySet()) {
      String key = argument.getKey();
      if (!key.startsWith(TABLE_PREFIX)) {
        continue;
      }
      String tableName = key.substring(TABLE_PREFIX.length());
      Schema tableSchema = Schema.parseJson(argument.getValue());

      String outputName = String.format("%s-%s", config.getReferenceName(), tableName);
      initOutput(context, outputName, tableName, tableSchema, bucket);
    }
  }

  @Override
  protected OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                         String tableName,
                                                         Schema tableSchema) {
    return new MultiSinkOutputFormatProvider(configuration, tableName, tableSchema, config.getSplitField());
  }

}
