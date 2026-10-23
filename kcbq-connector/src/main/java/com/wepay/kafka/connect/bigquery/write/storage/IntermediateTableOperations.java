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

package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Table creation and schema updates for the Storage Write API when upserts and deletes are applied
 * with a connector-issued MERGE. Records are written to intermediate tables, so each operation
 * applies to the intermediate table with the intermediate-table schema, and is then propagated to
 * the destination table, as {@link
 * com.wepay.kafka.connect.bigquery.write.row.UpsertDeleteBigQueryWriter} does for insertAll.
 */
public class IntermediateTableOperations {

  private final SchemaManager schemaManager;
  private final SchemaManager intermediateSchemaManager;
  private final Function<TableId, TableId> destinationTableFor;
  private final boolean autoCreateTables;

  /**
   * @param schemaManager the schema manager for destination tables
   * @param destinationTableFor maps an intermediate table to its destination table
   * @param autoCreateTables whether destination tables should be created automatically;
   *     intermediate tables are always created
   */
  public IntermediateTableOperations(
      SchemaManager schemaManager,
      Function<TableId, TableId> destinationTableFor,
      boolean autoCreateTables) {
    this.schemaManager = schemaManager;
    this.intermediateSchemaManager = schemaManager.forIntermediateTables();
    this.destinationTableFor = destinationTableFor;
    this.autoCreateTables = autoCreateTables;
  }

  /**
   * Creates the intermediate table and, if automatic table creation is enabled, creates or updates
   * its destination table.
   *
   * @param intermediateTable the intermediate table to create
   * @param records the records used to determine the schemas
   */
  public void createTable(TableId intermediateTable, List<SinkRecord> records) {
    intermediateSchemaManager.createTable(intermediateTable, records);
    if (autoCreateTables) {
      schemaManager.createOrUpdateTable(destinationTableFor.apply(intermediateTable), records);
    }
  }

  /**
   * Updates the schema of the intermediate table and of its destination table.
   *
   * @param intermediateTable the intermediate table to update
   * @param records the records used to determine the schemas
   */
  public void updateSchema(TableId intermediateTable, List<SinkRecord> records) {
    intermediateSchemaManager.updateSchema(intermediateTable, records);
    schemaManager.updateSchema(destinationTableFor.apply(intermediateTable), records);
  }
}
