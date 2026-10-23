/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.destTable;
import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryErrorResponses;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.utils.SleepUtils;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeQueries {

  public static final String INTERMEDIATE_TABLE_KEY_FIELD_NAME = "key";
  public static final String INTERMEDIATE_TABLE_VALUE_FIELD_NAME = "value";
  public static final String INTERMEDIATE_TABLE_ITERATION_FIELD_NAME = "i";
  public static final String INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME = "partitionTime";
  public static final String INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD = "batchNumber";
  public static final String DESTINATION_TABLE_ALIAS = "dstTableAlias";
  private static final int WAIT_MAX_JITTER = 1000;
  private static final Logger logger = LoggerFactory.getLogger(MergeQueries.class);

  private final MergeQueryBuilder mergeQueryBuilder;
  private final int bigQueryRetry;
  private final long bigQueryRetryWait;
  private final MergeBatches mergeBatches;
  private final KcbqThreadPoolExecutor executor;
  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final SinkTaskContext context;
  private final Time time;

  public MergeQueries(
      BigQuerySinkTaskConfig config,
      MergeBatches mergeBatches,
      KcbqThreadPoolExecutor executor,
      BigQuery bigQuery,
      SchemaManager schemaManager,
      SinkTaskContext context) {
    this(
        new DefaultMergeQueryBuilder(
            config
                .getKafkaKeyFieldName()
                .orElseThrow(
                    () ->
                        new ConnectException(
                            "Kafka key field must be configured when upsert/delete is enabled")),
            config.isStorageWriteApiMergeMode(),
            config.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG),
            config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG),
            config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)),
        config.getInt(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG),
        config.getLong(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG),
        mergeBatches,
        executor,
        bigQuery,
        schemaManager,
        context,
        Time.SYSTEM);
  }

  @VisibleForTesting
  MergeQueries(
      MergeQueryBuilder mergeQueryBuilder,
      int bigQueryRetry,
      long bigQueryRetryWait,
      MergeBatches mergeBatches,
      KcbqThreadPoolExecutor executor,
      BigQuery bigQuery,
      SchemaManager schemaManager,
      SinkTaskContext context,
      Time time) {
    this.mergeQueryBuilder = mergeQueryBuilder;
    this.bigQueryRetry = bigQueryRetry;
    this.bigQueryRetryWait = bigQueryRetryWait;
    this.mergeBatches = mergeBatches;
    this.executor = executor;
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.context = context;
    this.time = time;
  }

  // DELETE FROM `<dataset>`.`<intermediateTable>` WHERE batchNumber <= <batchNumber> AND
  // _PARTITIONTIME IS NOT NULL;
  @VisibleForTesting
  static String batchClearQuery(TableId intermediateTable, int batchNumber) {
    return new StringBuilder("DELETE FROM `")
        .append(intermediateTable.getDataset())
        .append("`.`")
        .append(intermediateTable.getTable())
        .append("` ")
        .append("WHERE ")
        .append(INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD)
        .append(" <= ")
        .append(batchNumber)
        .append(" ")
        // Use this clause to filter out rows that are still in the streaming buffer, which should
        // not be subjected to UPDATE or DELETE operations or the query will FAIL
        .append("AND _PARTITIONTIME IS NOT NULL")
        .append(";")
        .toString();
  }

  public void mergeFlushAll() {
    logger.debug("Triggering merge flush for all tables");
    mergeBatches.intermediateTables().forEach(this::mergeFlush);
  }

  public void mergeFlush(TableId intermediateTable) {
    if (mergeBatches.isCurrentBatchEmpty(intermediateTable)) {
      logger.debug("Merge flush is not performed as the current batch is empty.");
      return;
    }
    final TableId destinationTable = mergeBatches.destinationTableFor(intermediateTable);
    final int batchNumber = mergeBatches.incrementBatch(intermediateTable);
    logger.trace(
        "Triggering merge flush from {} to {} for batch {}",
        intTable(intermediateTable),
        destTable(destinationTable),
        batchNumber);

    executor.execute(
        () -> {
          try {
            mergeFlush(intermediateTable, destinationTable, batchNumber);
          } catch (InterruptedException e) {
            throw new ExpectedInterruptException(
                String.format(
                    "Interrupted while performing merge flush of batch %d from %s to %s",
                    batchNumber, intTable(intermediateTable), destTable(destinationTable)));
          }
        });
  }

  private void mergeFlush(TableId intermediateTable, TableId destinationTable, int batchNumber)
      throws InterruptedException {
    // If there are rows to flush in this batch, flush them
    if (mergeBatches.prepareToFlush(intermediateTable, batchNumber)) {
      logger.debug(
          "Running merge query on batch {} from {}", batchNumber, intTable(intermediateTable));
      String mergeFlushQuery = mergeFlushQuery(intermediateTable, destinationTable, batchNumber);
      logger.trace(mergeFlushQuery);

      int attempt = 0;
      boolean success = false;
      while (!success) {
        try {
          if (attempt > 0) {
            SleepUtils.waitRandomTime(time, this.bigQueryRetryWait, WAIT_MAX_JITTER);
          }
          bigQuery.query(QueryJobConfiguration.of(mergeFlushQuery));
          success = true;
        } catch (BigQueryException e) {
          if (attempt >= bigQueryRetry) {
            throw new BigQueryConnectException(
                "Failed to merge rows to destination table `"
                    + destinationTable
                    + "` within "
                    + attempt
                    + "  attempts.",
                e);
          } else if (BigQueryErrorResponses.isCouldNotSerializeAccessError(e)) {
            logger.warn(
                "Serialize access error while merging from {} to {}, retry attempt {}",
                intermediateTable,
                destinationTable,
                ++attempt);
          } else if (BigQueryErrorResponses.isJobInternalError(e)) {
            logger.warn(
                "Job internal error while merging from {} to {}, retry attempt {}",
                intermediateTable,
                destinationTable,
                ++attempt);
          } else if (BigQueryErrorResponses.isJobBackendError(e)) {
            logger.warn(
                "Job backend error while merging from {} to {}, retry attempt {}",
                intermediateTable,
                destinationTable,
                ++attempt);
          } else if (BigQueryErrorResponses.isJobRateLimitExceededError(e)) {
            logger.warn(
                "Job rate limit exceeded while merging from {} to {}, retry attempt {}",
                intermediateTable,
                destinationTable,
                ++attempt);
          } else if (BigQueryErrorResponses.isTableUnavailableError(e)) {
            logger.warn(
                "Table unavailable error while merging from {} to {}, retry attempt {}",
                intermediateTable,
                destinationTable,
                ++attempt);
          } else {
            throw e;
          }
        }
      }
      logger.trace(
          "Merge from {} to {} completed",
          intTable(intermediateTable),
          destTable(destinationTable));

      logger.debug(
          "Recording flush success for batch {} from {}", batchNumber, intTable(intermediateTable));
      mergeBatches.recordSuccessfulFlush(intermediateTable, batchNumber);

      // Commit those offsets ASAP
      context.requestCommit();

      logger.info(
          "Completed merge flush of batch {} from {} to {}",
          batchNumber,
          intTable(intermediateTable),
          destTable(destinationTable));
    }

    // After, regardless of whether we flushed or not, clean up old batches from the intermediate
    // table. Some rows may be several batches old but still in the table if they were in the
    // streaming buffer during the last purge.
    logger.trace(
        "Clearing batches from {} on back from {}", batchNumber, intTable(intermediateTable));
    String batchClearQuery = batchClearQuery(intermediateTable, batchNumber);
    logger.trace(batchClearQuery);
    bigQuery.query(QueryJobConfiguration.of(batchClearQuery));
  }

  @VisibleForTesting
  String mergeFlushQuery(TableId intermediateTable, TableId destinationTable, int batchNumber) {
    return mergeQueryBuilder.build(
        intermediateTable,
        destinationTable,
        batchNumber,
        schemaManager.cachedSchema(intermediateTable));
  }
}
