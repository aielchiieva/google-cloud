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

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.bigquery.util.BigQueryUtil;
import co.cask.hydrator.common.LineageRecorder;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base class for Big Query batch sink plugins.
 */
public abstract class AbstractBigQuerySink extends BatchSink<StructuredRecord, JsonObject, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBigQuerySink.class);

  private static final String gcsPathFormat = "gs://%s";
  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

  // UUID for the run. Will be used as bucket name if bucket is not provided.
  private UUID uuid;
  private Configuration baseConfiguration;

  @Override
  public final void prepareRun(BatchSinkContext context) throws Exception {
    prepareRunValidation(context);

    createDataset(context.isPreviewEnabled());

    uuid = UUID.randomUUID();
    baseConfiguration = BigQueryUtil.getBigQueryConfig(getConfig().getServiceAccountFilePath(),
                                                       getConfig().getProject());
    String bucket = configureBucket();

    prepareRunInternal(context, bucket);
  }

  @Override
  public final void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (getConfig().getBucket() == null) {
      Path gcsPath = new Path(String.format(gcsPathFormat, uuid.toString()));
      try {
        FileSystem fs = gcsPath.getFileSystem(baseConfiguration);
        if (fs.exists(gcsPath)) {
          fs.delete(gcsPath, true);
          LOG.debug("Deleted temporary bucket '{}'", gcsPath);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete bucket '{}': {}", gcsPath, e.getMessage());
      }
    }
  }

  protected final void initOutput(BatchSinkContext context, String outputName,
                            String tableName, Schema tableSchema, String bucket) throws IOException {
    LOG.debug("Init output for table '{}' with schema: {}", tableName, tableSchema);
    validateSchema(tableName, tableSchema);
    List<BigQueryTableFieldSchema> fields = getBigQueryTableFields(tableSchema);
    Configuration configuration = getOutputConfiguration(bucket, tableName, fields);

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exist.
    // We call emitLineage before since it creates the dataset with schema which is used.
    List<String> fieldNames = fields.stream()
      .map(BigQueryTableFieldSchema::getName)
      .collect(Collectors.toList());
    recordLineage(context, outputName, tableSchema, fieldNames);
    context.addOutput(Output.of(outputName, getOutputFormatProvider(configuration, tableName, tableSchema)));
  }

  protected final void decodeSimpleTypes(JsonObject json, String name, StructuredRecord input) {
    Object object = input.get(name);
    Schema.Field field = input.getSchema().getField(name);

    if (field == null) {
      throw new IllegalStateException(String.format("Field '%s' is absent in input record", name));
    }

    Schema schema = BigQueryUtil.getNonNullableSchema(field.getSchema());

    if (object == null) {
      json.add(name, JsonNull.INSTANCE);
      return;
    }

    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          json.addProperty(name, Objects.requireNonNull(input.getDate(name)).toString());
          break;
        case TIME_MILLIS:
        case TIME_MICROS:
          json.addProperty(name, timeFormatter.format(Objects.requireNonNull(input.getTime(name))));
          break;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          //timestamp for json input should be in this format yyyy-MM-dd HH:mm:ss.SSSSSS
          json.addProperty(name, dateTimeFormatter.format(Objects.requireNonNull(input.getTimestamp(name))));
          break;
        default:
          throw new IllegalStateException(
            String.format("Field '%s' is of unsupported type '%s'", name, logicalType.getToken()));
      }
      return;
    }

    switch (schema.getType()) {
      case NULL:
        json.add(name, JsonNull.INSTANCE); // nothing much to do here.
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        json.addProperty(name, (Number) object);
        break;
      case BOOLEAN:
        json.addProperty(name, (Boolean) object);
        break;
      case STRING:
        json.addProperty(name, object.toString());
        break;
      default:
        throw new IllegalStateException(
          String.format("Field '%s' is of unsupported type '%s'", name, schema.getType()));
    }
  }

  protected abstract AbstractBigQuerySinkConfig getConfig();
  protected abstract void prepareRunValidation(BatchSinkContext context);
  protected abstract void prepareRunInternal(BatchSinkContext context, String bucket) throws IOException;
  protected abstract OutputFormatProvider getOutputFormatProvider(Configuration configuration,
                                                                  String tableName,
                                                                  Schema tableSchema);

  private void createDataset(boolean previewEnabled) throws IOException {
    if (previewEnabled) {
      BigQuery bigquery = BigQueryUtil.getBigQuery(getConfig().getServiceAccountFilePath(), getConfig().getProject());
      // create dataset if it does not exist
      if (bigquery.getDataset(getConfig().getDataset()) == null) {
        try {
          bigquery.create(DatasetInfo.newBuilder(getConfig().getDataset()).build());
        } catch (BigQueryException e) {
          throw new IllegalStateException("Exception occurred while creating dataset: " + getConfig().getDataset(), e);
        }
      }
    }
  }

  private String getTemporaryGcsPath(String bucket, String tableName) {
    return String.format(gcsPathFormat + "/hadoop/input/%s-%s", bucket, tableName, uuid);
  }

  private String configureBucket() {
    String bucket = getConfig().getBucket();
    if (getConfig().getBucket() == null) {
      bucket = uuid.toString();
      // By default, this option is false, meaning the job can not delete the bucket.
      // So enable it only when bucket name is not provided.
      baseConfiguration.setBoolean("fs.gs.bucket.delete.enable", true);
    }
    baseConfiguration.set("fs.gs.system.bucket", bucket);
    baseConfiguration.setBoolean("fs.gs.impl.disable.cache", true);
    baseConfiguration.setBoolean("fs.gs.metadata.cache.enable", false);
    return bucket;
  }

  /**
   * Validates output schema against Big Query table schema. It throws {@link IllegalArgumentException}
   * if the output schema has more fields than Big Query table or output schema field types does not match
   * Big Query column types.
   */
  private void validateSchema(String tableName, Schema tableSchema) throws IOException {
    Table table = BigQueryUtil.getBigQueryTable(getConfig().getServiceAccountFilePath(),
                                                getConfig().getProject(),
                                                getConfig().getDataset(), tableName);
    if (table == null) {
      // Table does not exist, so no further validation is required.
      return;
    }

    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null) {
      // Table is created without schema, so no further validation is required.
      return;
    }

    FieldList bqFields = bqSchema.getFields();
    List<Schema.Field> outputSchemaFields = Objects.requireNonNull(tableSchema.getFields());

    // Output schema should not have fields that are not present in Big Query table.
    List<String> diff = BigQueryUtil.getSchemaMinusBqFields(outputSchemaFields, bqFields);
    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("The output schema does not match the BigQuery table schema for '%s.%s' table. " +
                        "The table does not contain the '%s' column(s).",
                      getConfig().getDataset(), table, diff));
    }

    // validate the missing columns in output schema are nullable fields in Big Query
    List<String> remainingBQFields = BigQueryUtil.getBqFieldsMinusSchema(bqFields, outputSchemaFields);
    for (String field : remainingBQFields) {
      if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
        throw new IllegalArgumentException(
          String.format("The output schema does not match the BigQuery table schema for '%s.%s'. " +
                          "The table requires column '%s', which is not in the output schema.",
                        getConfig().getDataset(), tableName, field));
      }
    }

    // Match output schema field type with Big Query column type
    for (Schema.Field field : tableSchema.getFields()) {
      BigQueryUtil.validateFieldSchemaMatches(bqFields.get(field.getName()),
                                              field, getConfig().getDataset(), tableName);
    }
  }

  private List<BigQueryTableFieldSchema> getBigQueryTableFields(Schema tableSchema) {
    return Objects.requireNonNull(tableSchema.getFields()).stream()
      .map(field -> new BigQueryTableFieldSchema()
        .setName(field.getName())
        .setType(getTableDataType(BigQueryUtil.getNonNullableSchema(field.getSchema())).name())
        .setMode(Field.Mode.NULLABLE.name()))
      .collect(Collectors.toList());
  }

  private Configuration getOutputConfiguration(String bucket,
                                               String tableName,
                                               List<BigQueryTableFieldSchema> fields) throws IOException {
    Configuration configuration = new Configuration(baseConfiguration);
    String temporaryGcsPath = getTemporaryGcsPath(bucket, tableName);

    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s.%s", getConfig().getDataset(), tableName),
      new BigQueryTableSchema().setFields(fields),
      temporaryGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      TextOutputFormat.class);

    return configuration;
  }

  private void recordLineage(BatchSinkContext context,
                             String outputName,
                             Schema tableSchema,
                             List<String> fieldNames) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to BigQuery table.", fieldNames);
    }
  }

  private LegacySQLTypeName getTableDataType(Schema schema) {
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return LegacySQLTypeName.DATE;
        case TIME_MILLIS:
        case TIME_MICROS:
          return LegacySQLTypeName.TIME;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return LegacySQLTypeName.TIMESTAMP;
        default:
          throw new IllegalStateException("Unsupported type " + logicalType.getToken());
      }
    }

    Schema.Type type = schema.getType();
    switch (type) {
      case INT:
      case LONG:
        return LegacySQLTypeName.INTEGER;
      case STRING:
        return LegacySQLTypeName.STRING;
      case FLOAT:
      case DOUBLE:
        return LegacySQLTypeName.FLOAT;
      case BOOLEAN:
        return LegacySQLTypeName.BOOLEAN;
      case BYTES:
        return LegacySQLTypeName.BYTES;
      default:
        throw new IllegalStateException("Unsupported type " + type);
    }
  }

}
