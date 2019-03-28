/*
 * Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package co.cask.gcp.bigquery.sink;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.io.bigquery.BigQueryUtils;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputCommitter;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Extends {@link IndirectBigQueryOutputFormat} to provide {@link ExtendedIndirectBigQueryOutputCommitter}
 * in {@link #createCommitter(TaskAttemptContext)} method.
 *
 * @param <K> key
 * @param <V> value
 */
public class ExtendedIndirectBigQueryOutputFormat<K, V> extends IndirectBigQueryOutputFormat<K, V> {

  /**
   * Wraps the delegate's committer in a {@link ExtendedIndirectBigQueryOutputCommitter}.
   *
   * @param context task context
   * @return output committer
   */
  @Override
  public OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
    return new ExtendedIndirectBigQueryOutputCommitter(context, delegateCommitter);
  }

  /**
   * Extends {@link IndirectBigQueryOutputCommitter} and preserves all its original functionality,
   * except of wrapping origin Big Query helper into custom one which allows better job configuration option control.
   */
  private static class ExtendedIndirectBigQueryOutputCommitter extends IndirectBigQueryOutputCommitter {

    private final BigQueryHelper bigQueryHelper;

    ExtendedIndirectBigQueryOutputCommitter(TaskAttemptContext context, OutputCommitter delegate) throws IOException {
      super(context, delegate);
      // get original Big Query helper and wrap into custom implementation
      // store it on the class level to return instead of original one
      BigQueryHelper bigQueryHelper = super.getBigQueryHelper();
      this.bigQueryHelper = new ExtendedBigQueryHelper(bigQueryHelper.getRawBigquery());
    }

    /**
     * Overrides original getter to pass custom Big Query helper.
     *
     * @return custom big query helper
     */
    @Override
    protected BigQueryHelper getBigQueryHelper() {
      return bigQueryHelper;
    }

    /**
     * Extends {@link BigQueryHelper} and overrides its {@link BigQueryHelper#importFromGcs} method,
     * preserving most of original functionality, except of allowing better job configuration option control.
     * For instance, allowing schema relaxation policy.
     */
    private static class ExtendedBigQueryHelper extends BigQueryHelper {

      private static final Logger LOG = LoggerFactory.getLogger(ExtendedBigQueryHelper.class);

      private static final Progressable NOP_PROGRESSABLE = () -> { };

      ExtendedBigQueryHelper(Bigquery service) {
        super(service);
      }

      @Override
      public void importFromGcs(String projectId,
                                TableReference tableRef,
                                @Nullable TableSchema schema,
                                @Nullable String kmsKeyName,
                                BigQueryFileFormat sourceFormat,
                                String writeDisposition,
                                List<String> gcsPaths,
                                boolean awaitCompletion) throws IOException, InterruptedException {
        LOG.info("Importing into table '%s' from %s paths; path[0] is '%s'; awaitCompletion: %s",
          BigQueryStrings.toString(tableRef),
          gcsPaths.size(),
          gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0),
          awaitCompletion);

        JobConfigurationLoad loadConfig = new JobConfigurationLoad()
          .setSchema(schema)
          .setSourceFormat(sourceFormat.getFormatIdentifier())
          .setSourceUris(gcsPaths)
          .setDestinationTable(tableRef)
          .setWriteDisposition(writeDisposition)
          // allow schema relaxation
          .setSchemaUpdateOptions(Arrays.asList(
            JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION.name(),
            JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION.name()));

        if (!Strings.isNullOrEmpty(kmsKeyName)) {
          loadConfig.setDestinationEncryptionConfiguration(new EncryptionConfiguration().setKmsKeyName(kmsKeyName));
        }

        if (schema == null) {
          loadConfig.setAutodetect(true);
        }

        JobConfiguration config = new JobConfiguration();
        config.setLoad(loadConfig);

        Dataset dataset = getRawBigquery().datasets().get(tableRef.getProjectId(), tableRef.getDatasetId()).execute();
        JobReference jobReference =
          createJobReference(projectId, "direct-bigqueryhelper-import", dataset.getLocation());

        Job job = new Job();
        job.setConfiguration(config);
        job.setJobReference(jobReference);

        insertJobOrFetchDuplicate(projectId, job);

        if (awaitCompletion) {
          BigQueryUtils.waitForJobCompletion(getRawBigquery(), projectId, jobReference, NOP_PROGRESSABLE);
        }
      }
    }

  }

}
