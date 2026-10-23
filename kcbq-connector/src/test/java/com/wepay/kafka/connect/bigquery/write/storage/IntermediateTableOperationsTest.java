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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class IntermediateTableOperationsTest {
  private static final TableId INTERMEDIATE_TABLE = TableId.of("dataset", "topic_tmp_1");
  private static final TableId DESTINATION_TABLE = TableId.of("dataset", "topic");
  private static final List<SinkRecord> RECORDS = Collections.singletonList(mock(SinkRecord.class));

  private final SchemaManager schemaManager = mock(SchemaManager.class);
  private final SchemaManager intermediateSchemaManager = mock(SchemaManager.class);

  @BeforeEach
  public void setUp() {
    when(schemaManager.forIntermediateTables()).thenReturn(intermediateSchemaManager);
  }

  @Test
  public void testCreateTableCreatesIntermediateThenDestination() {
    operations(true).createTable(INTERMEDIATE_TABLE, RECORDS);

    InOrder inOrder = inOrder(intermediateSchemaManager, schemaManager);
    inOrder.verify(intermediateSchemaManager).createTable(INTERMEDIATE_TABLE, RECORDS);
    inOrder.verify(schemaManager).createOrUpdateTable(DESTINATION_TABLE, RECORDS);
  }

  @Test
  public void testCreateTableWithoutAutoCreateLeavesDestinationAlone() {
    operations(false).createTable(INTERMEDIATE_TABLE, RECORDS);

    // Intermediate tables are always created; the destination must already exist
    verify(intermediateSchemaManager).createTable(INTERMEDIATE_TABLE, RECORDS);
    verify(schemaManager, never()).createOrUpdateTable(any(), any());
    verify(schemaManager, never()).createTable(any(), any());
  }

  @Test
  public void testUpdateSchemaUpdatesIntermediateThenDestination() {
    operations(false).updateSchema(INTERMEDIATE_TABLE, RECORDS);

    InOrder inOrder = inOrder(intermediateSchemaManager, schemaManager);
    inOrder.verify(intermediateSchemaManager).updateSchema(INTERMEDIATE_TABLE, RECORDS);
    inOrder.verify(schemaManager).updateSchema(DESTINATION_TABLE, RECORDS);
  }

  private IntermediateTableOperations operations(boolean autoCreateTables) {
    return new IntermediateTableOperations(
        schemaManager, Map.of(INTERMEDIATE_TABLE, DESTINATION_TABLE)::get, autoCreateTables);
  }
}
