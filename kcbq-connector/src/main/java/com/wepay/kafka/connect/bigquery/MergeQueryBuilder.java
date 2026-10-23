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

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;

/**
 * Builds the query that merges one batch of an intermediate table into its destination table.
 * {@link MergeQueries} runs the query, with retries, and then records the flush and requests an
 * offset commit; implementations only decide the SQL, for example to add a partition-pruning
 * predicate.
 */
public interface MergeQueryBuilder {

  /**
   * @param intermediateTable the intermediate table holding the batch
   * @param destinationTable the table to merge the batch into
   * @param batchNumber the batch to merge; rows of other batches must be ignored
   * @param intermediateSchema the intermediate table's schema, with nested {@code key} and {@code
   *     value} structs
   * @return the query to run; may be a multi-statement script (for example {@code DECLARE} / {@code
   *     SET} / {@code MERGE}), as long as it applies the batch exactly once
   */
  String build(
      TableId intermediateTable,
      TableId destinationTable,
      int batchNumber,
      Schema intermediateSchema);
}
