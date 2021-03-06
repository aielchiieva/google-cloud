/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * {@link GCSMultiBatchSink} that stores the data of the latest run of an adapter in GCS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSMultiFiles")
@Description("Writes records to one or more avro files in a directory on Google Cloud Storage.")
public class GCSMultiBatchSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final String TABLE_PREFIX = "multisink.";
  private static final String FORMAT_PLUGIN_ID = "format";
  private static final String SCHEMA_MACRO = "__provided_schema__";

  private final GCSMultiBatchSinkConfig config;

  public GCSMultiBatchSink(GCSMultiBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    FileFormat format = config.getFormat();
    // add schema as a macro since we don't know it until runtime
    PluginProperties formatProperties = PluginProperties.builder()
      .addAll(config.getProperties().getProperties())
      .add("schema", String.format("${%s}", SCHEMA_MACRO)).build();
    OutputFormatProvider outputFormatProvider =
      pipelineConfigurer.usePlugin(BatchSink.FORMAT_PLUGIN_TYPE, format.name().toLowerCase(),
                                   FORMAT_PLUGIN_ID, formatProperties);
    if (outputFormatProvider == null) {
      throw new IllegalArgumentException(String.format("Could not find the '%s' output format plugin.",
                                                       format.name().toLowerCase()));
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws IOException, InstantiationException {
    config.validate();
    Map<String, String> baseProperties = new HashMap<>(GCPUtils.getFileSystemProperties(config));

    Map<String, String> argumentCopy = new HashMap<>(context.getArguments().asMap());
    for (Map.Entry<String, String> argument : argumentCopy.entrySet()) {
      String key = argument.getKey();
      if (!key.startsWith(TABLE_PREFIX)) {
        continue;
      }
      String name = key.substring(TABLE_PREFIX.length());
      Schema schema = Schema.parseJson(argument.getValue());
      // TODO: (CDAP-14600) pass in schema as an argument instead of using macros and setting arguments
      // add better platform support to allow passing in arguments when instantiating a plugin
      context.getArguments().set(SCHEMA_MACRO, schema.toString());
      OutputFormatProvider outputFormatProvider = context.newPluginInstance(FORMAT_PLUGIN_ID);

      Map<String, String> outputProperties = new HashMap<>(baseProperties);
      outputProperties.putAll(outputFormatProvider.getOutputFormatConfiguration());
      outputProperties.putAll(RecordFilterOutputFormat.configure(outputFormatProvider.getOutputFormatClassName(),
                                                                 config.splitField, name, schema));
      outputProperties.put(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime(), name));

      context.addOutput(Output.of(
        config.getReferenceName() + "_" + name,
        new SinkOutputFormatProvider(RecordFilterOutputFormat.class.getName(), outputProperties)));
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  /**
   * Sink configuration.
   */
  public static class GCSMultiBatchSinkConfig extends GCSBatchSink.GCSBatchSinkConfig {

    @Description("The codec to use when writing data. " +
      "The 'avro' format supports 'snappy' and 'deflate'. The parquet format supports 'snappy' and 'gzip'. " +
      "Other formats do not support compression.")
    @Nullable
    private String compressionCodec;

    @Description("The name of the field that will be used to determine which directory to write to.")
    private String splitField = "tablename";

    protected String getOutputDir(long logicalStartTime, String context) {
      boolean suffixOk = !Strings.isNullOrEmpty(getSuffix());
      String timeSuffix = suffixOk ? new SimpleDateFormat(getSuffix()).format(logicalStartTime) : "";
      return String.format("%s/%s/%s", getPath(), context, timeSuffix);
    }

  }
}
