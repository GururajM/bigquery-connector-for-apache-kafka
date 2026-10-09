/*
 * Copyright 2022-2026 Aiven Oy and
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
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.config;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DELETE_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.STORAGE_WRITE_API_UPSERT_MODE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.UPSERT_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UpsertDeleteValidator extends MultiPropertyValidator<BigQuerySinkConfig> {
  private static final Collection<String> DEPENDENTS =
      Collections.unmodifiableCollection(
          Arrays.asList(
              MERGE_INTERVAL_MS_CONFIG,
              MERGE_RECORDS_THRESHOLD_CONFIG,
              KAFKA_KEY_FIELD_NAME_CONFIG,
              USE_STORAGE_WRITE_API_CONFIG,
              STORAGE_WRITE_API_UPSERT_MODE_CONFIG,
              TRACK_PUT_ATTEMPTS_CONFIG,
              ENABLE_BATCH_MODE_CONFIG));
  private static final Logger logger = LoggerFactory.getLogger(UpsertDeleteValidator.class);

  public static final String MERGE_MODE_TRACK_PUT_ATTEMPTS_ERROR =
      TRACK_PUT_ATTEMPTS_CONFIG
          + " is not supported when "
          + STORAGE_WRITE_API_UPSERT_MODE_CONFIG
          + "=MERGE; either disable "
          + TRACK_PUT_ATTEMPTS_CONFIG
          + " or set "
          + STORAGE_WRITE_API_UPSERT_MODE_CONFIG
          + " to CDC";
  public static final String MERGE_MODE_BATCH_MODE_ERROR =
      ENABLE_BATCH_MODE_CONFIG
          + " is not supported when "
          + STORAGE_WRITE_API_UPSERT_MODE_CONFIG
          + "=MERGE; either disable "
          + ENABLE_BATCH_MODE_CONFIG
          + " or set "
          + STORAGE_WRITE_API_UPSERT_MODE_CONFIG
          + " to CDC";

  private UpsertDeleteValidator(String propertyName) {
    super(propertyName);
  }

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    if (!modeEnabled(config)) {
      return Optional.empty();
    }

    if (!config.useStorageWriteApi()) {
      // Classic MERGE path: merge interval, threshold and kafkaKeyFieldName are all required
      long mergeInterval = config.getLong(MERGE_INTERVAL_MS_CONFIG);
      long mergeRecordsThreshold = config.getLong(MERGE_RECORDS_THRESHOLD_CONFIG);

      if (mergeInterval == -1 && mergeRecordsThreshold == -1) {
        return Optional.of(
            String.format(
                "%s and %s cannot both be -1 when %s is false",
                MERGE_INTERVAL_MS_CONFIG,
                MERGE_RECORDS_THRESHOLD_CONFIG,
                USE_STORAGE_WRITE_API_CONFIG));
      }

    } else {
      if (config.isStorageWriteApiMergeMode()) {
        // Retries re-convert rows as regular rows and would add them to the merge batch again
        if (config.getBoolean(TRACK_PUT_ATTEMPTS_CONFIG)) {
          return Optional.of(MERGE_MODE_TRACK_PUT_ATTEMPTS_ERROR);
        }
        // Pending streams make rows visible only on commit, which merge batches do not track
        if (config.getBoolean(ENABLE_BATCH_MODE_CONFIG)) {
          return Optional.of(MERGE_MODE_BATCH_MODE_ERROR);
        }
      }

      // TODO: remove once records are routed through the intermediate table and merge batches
      // track Storage Write API writes; until then MERGE mode would never commit offsets
      if (config.isStorageWriteApiMergeMode()) {
        return Optional.of(
            String.format(
                "%s=%s is not yet supported",
                STORAGE_WRITE_API_UPSERT_MODE_CONFIG,
                BigQuerySinkConfig.StorageWriteApiUpsertMode.MERGE.name()));
      }

      // Storage Write API CDC path
      for (String property :
          Arrays.asList(MERGE_INTERVAL_MS_CONFIG, MERGE_RECORDS_THRESHOLD_CONFIG)) {
        if (config.originals().containsKey(property)) {
          logger.warn(
              "The {} property will be ignored because {} is set to true",
              property,
              USE_STORAGE_WRITE_API_CONFIG);
        }
      }

      // Delete-only mode is not supported with the Storage Write API
      if (!config.isUpsertEnabled() && config.isDeleteEnabled()) {
        return Optional.of(
            String.format(
                "Delete-only mode is not supported when the Storage Write API is enabled "
                    + "(%s = true); please either disable delete support (set %s to false) "
                    + "or enable upsert support (set %s to true)",
                USE_STORAGE_WRITE_API_CONFIG, DELETE_ENABLED_CONFIG, UPSERT_ENABLED_CONFIG));
      }
    }

    return Optional.empty();
  }

  /**
   * @param config the user-provided configuration
   * @return whether the write mode for the validator (i.e., either upsert or delete) is enabled
   */
  protected abstract boolean modeEnabled(BigQuerySinkConfig config);

  public static class UpsertValidator extends UpsertDeleteValidator {
    public UpsertValidator() {
      super(UPSERT_ENABLED_CONFIG);
    }

    @Override
    protected boolean modeEnabled(BigQuerySinkConfig config) {
      return config.getBoolean(UPSERT_ENABLED_CONFIG);
    }
  }

  public static class DeleteValidator extends UpsertDeleteValidator {
    public DeleteValidator() {
      super(DELETE_ENABLED_CONFIG);
    }

    @Override
    protected boolean modeEnabled(BigQuerySinkConfig config) {
      return config.getBoolean(DELETE_ENABLED_CONFIG);
    }
  }
}
