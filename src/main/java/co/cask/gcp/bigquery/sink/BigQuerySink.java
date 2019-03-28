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
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.bigquery.util.BigQueryUtil;

import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * This class <code>BigQuerySink</code> is a plugin that would allow users
 * to write <code>StructuredRecords</code> to Google Big Query.
 *
 * The plugin uses native BigQuery Output format to write data.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(BigQuerySink.NAME)
@Description("This sink writes to a BigQuery table. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse. "
  + "Data is first written to a temporary location on Google Cloud Storage, then loaded into BigQuery from there.")
public final class BigQuerySink extends AbstractBigQuerySink {

  public static final String NAME = "BigQueryTable";

  private final BigQuerySinkConfig config;
  private Schema schema;

  public BigQuerySink(BigQuerySinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema());
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    schema = config.getSchema();
  }

  @Override
  protected BigQuerySinkConfig getConfig() {
    return config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<JsonObject, NullWritable>> emitter) {
    JsonObject object = new JsonObject();
    for (Schema.Field recordField : Objects.requireNonNull(input.getSchema().getFields())) {
      // From all the fields in input record, decode only those fields that are present in output schema
      if (schema.getField(recordField.getName()) != null) {
        decodeSimpleTypes(object, recordField.getName(), input);
      }
    }
    emitter.emit(new KeyValue<>(object, NullWritable.get()));
  }

  @Override
  protected void prepareRunValidation(BatchSinkContext context) {
    config.validate(context.getInputSchema());
  }

  @Override
  protected void prepareRunInternal(BatchSinkContext context, String bucket) throws IOException {
    initOutput(context, config.getReferenceName(), config.getTable(), config.getSchema(), bucket);
  }

  @Override
  protected OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                         String tableName,
                                                         Schema tableSchema) {
    return new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return IndirectBigQueryOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        return BigQueryUtil.configToMap(configuration);
      }
    };
  }

}
