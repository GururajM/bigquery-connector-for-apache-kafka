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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MergeQueriesTest {

  private static final String KEY = "kafkaKey";

  private static final int BATCH_NUMBER = 42;
  private static final int BIGQUERY_RETRY = 3;
  private static final int BIGQUERY_RETRY_WAIT = 1000;
  private static final TableId DESTINATION_TABLE = TableId.of("ds1", "t");
  private static final TableId INTERMEDIATE_TABLE = TableId.of("ds1", "t_tmp_6_uuid_epoch");
  private static final Schema INTERMEDIATE_TABLE_SCHEMA = constructIntermediateTable();

  private static final SinkRecord TEST_SINK_RECORD =
      new SinkRecord("test", 0, null, null, null, null, 0);
  private final Time time = new MockTime();
  private MergeBatches mergeBatches = mock(MergeBatches.class);
  private final KcbqThreadPoolExecutor executor = mock(KcbqThreadPoolExecutor.class);
  private final BigQuery bigQuery = mock(BigQuery.class);
  private final SchemaManager schemaManager = mock(SchemaManager.class);
  private final SinkTaskContext context = mock(SinkTaskContext.class);

  private static Schema constructIntermediateTable() {
    List<Field> fields = new ArrayList<>();

    List<Field> valueFields =
        Arrays.asList(
            Field.of("f1", LegacySQLTypeName.STRING),
            Field.of(
                "f2", LegacySQLTypeName.RECORD, Field.of("nested_f1", LegacySQLTypeName.INTEGER)),
            Field.of("f3", LegacySQLTypeName.BOOLEAN),
            Field.of("f4", LegacySQLTypeName.BYTES));
    Field wrappedValueField =
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME,
                LegacySQLTypeName.RECORD,
                valueFields.toArray(new Field[0]))
            .setMode(Field.Mode.NULLABLE)
            .build();
    fields.add(wrappedValueField);

    List<Field> keyFields =
        Arrays.asList(
            Field.of("k1", LegacySQLTypeName.STRING),
            Field.of(
                "k2",
                LegacySQLTypeName.RECORD,
                Field.of(
                    "nested_k1",
                    LegacySQLTypeName.RECORD,
                    Field.of("doubly_nested_k", LegacySQLTypeName.BOOLEAN)),
                Field.of("nested_k2", LegacySQLTypeName.INTEGER)));
    Field kafkaKeyField =
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME,
                LegacySQLTypeName.RECORD,
                keyFields.toArray(new Field[0]))
            .setMode(Field.Mode.REQUIRED)
            .build();
    fields.add(kafkaKeyField);

    Field partitionTimeField =
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME,
                LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build();
    fields.add(partitionTimeField);

    Field batchNumberField =
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER)
            .setMode(Field.Mode.REQUIRED)
            .build();
    fields.add(batchNumberField);

    return Schema.of(fields);
  }

  @BeforeEach
  public void setUp() {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE)).thenReturn(INTERMEDIATE_TABLE_SCHEMA);
  }

  private MergeQueries mergeQueries(boolean insertPartitionTime, boolean upsert, boolean delete) {
    return mergeQueries(
        new DefaultMergeQueryBuilder(KEY, false, insertPartitionTime, upsert, delete));
  }

  private MergeQueries mergeQueries(MergeQueryBuilder mergeQueryBuilder) {
    return new MergeQueries(
        mergeQueryBuilder,
        BIGQUERY_RETRY,
        BIGQUERY_RETRY_WAIT,
        mergeBatches,
        executor,
        bigQuery,
        schemaManager,
        context,
        time);
  }

  private MergeQueries flattenedMergeQueries(
      boolean insertPartitionTime, boolean upsert, boolean delete) {
    return mergeQueries(
        new DefaultMergeQueryBuilder("", true, insertPartitionTime, upsert, delete));
  }

  /** Staging schema whose key field k1 is also in the value, as with Debezium-style records. */
  private static Schema flattenedIntermediateSchema(
      List<Field> keyFields, List<Field> valueFields) {
    return Schema.of(
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME,
                LegacySQLTypeName.RECORD,
                valueFields.toArray(new Field[0]))
            .setMode(Field.Mode.NULLABLE)
            .build(),
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME,
                LegacySQLTypeName.RECORD,
                keyFields.toArray(new Field[0]))
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.of(
            MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP),
        Field.newBuilder(
                MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER)
            .setMode(Field.Mode.REQUIRED)
            .build());
  }

  private static final List<Field> FLATTENED_KEY_FIELDS =
      Arrays.asList(
          Field.of("k1", LegacySQLTypeName.STRING),
          Field.of(
              "k2", LegacySQLTypeName.RECORD, Field.of("nested_k", LegacySQLTypeName.INTEGER)));
  private static final List<Field> FLATTENED_VALUE_FIELDS =
      Arrays.asList(
          Field.of("k1", LegacySQLTypeName.STRING),
          Field.of("f1", LegacySQLTypeName.STRING),
          Field.of("f2", LegacySQLTypeName.INTEGER));

  private String flattenedQuery(boolean insertPartitionTime, boolean upsert, boolean delete) {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE))
        .thenReturn(flattenedIntermediateSchema(FLATTENED_KEY_FIELDS, FLATTENED_VALUE_FIELDS));
    return flattenedMergeQueries(insertPartitionTime, upsert, delete)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
  }

  @Test
  public void testCustomMergeQueryBuilderIsUsed() {
    // A pruning builder may return a script that bounds the batch before the MERGE
    String script = "DECLARE keyMin INT64; SET keyMin = 1; MERGE ...;";
    MergeQueryBuilder mergeQueryBuilder = mock(MergeQueryBuilder.class);
    when(mergeQueryBuilder.build(
            INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER, INTERMEDIATE_TABLE_SCHEMA))
        .thenReturn(script);

    assertEquals(
        script,
        mergeQueries(mergeQueryBuilder)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER));
  }

  @Test
  public void testFlattenedUpsertQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " dstTableAlias "
            + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM "
            + table(INTERMEDIATE_TABLE)
            + " x "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + " "
            + "GROUP BY key.k1, key.k2.nested_k)) "
            + "ON dstTableAlias.`k1`=src.key.k1 AND dstTableAlias.`k2`=src.key.k2 "
            + "WHEN MATCHED "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2 "
            + "WHEN NOT MATCHED "
            + "THEN INSERT (`k1`, `k2`, _PARTITIONTIME, `f1`, `f2`) "
            + "VALUES (src.key.k1, src.key.k2, "
            + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
            + "src.value.f1, src.value.f2);";
    assertEquals(expectedQuery, flattenedQuery(true, true, false));
  }

  @Test
  public void testFlattenedUpsertDeleteQuery() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " dstTableAlias "
            + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM "
            + table(INTERMEDIATE_TABLE)
            + " x "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + " "
            + "GROUP BY key.k1, key.k2.nested_k)) "
            + "ON dstTableAlias.`k1`=src.key.k1 AND dstTableAlias.`k2`=src.key.k2 "
            + "WHEN MATCHED AND src.value IS NOT NULL "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2 "
            + "WHEN MATCHED AND src.value IS NULL "
            + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`k1`, `k2`, `f1`, `f2`) "
            + "VALUES (src.key.k1, src.key.k2, src.value.f1, src.value.f2);";
    assertEquals(expectedQuery, flattenedQuery(false, true, true));
  }

  @Test
  public void testFlattenedDeleteQuery() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " "
            + "USING (SELECT batch.key AS key, value "
            + "FROM (SELECT src.i, src.key FROM ("
            + "SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM (SELECT * FROM "
            + table(INTERMEDIATE_TABLE)
            + " "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + ") x "
            + "WHERE x.value IS NULL "
            + "GROUP BY key.k1, key.k2.nested_k)) AS deletes "
            + "RIGHT JOIN ("
            + "SELECT * FROM "
            + table(INTERMEDIATE_TABLE)
            + " "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + ") AS batch "
            + "USING (key) "
            + "WHERE deletes.i IS NULL OR batch.i >= deletes.i "
            + "ORDER BY batch.i ASC) AS src "
            + "ON `"
            + DESTINATION_TABLE.getTable()
            + "`.`k1`=src.key.k1 AND `"
            + DESTINATION_TABLE.getTable()
            + "`.`k2`=src.key.k2 AND src.value IS NULL "
            + "WHEN MATCHED "
            + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`k1`, `k2`, `f1`, `f2`) "
            + "VALUES (src.key.k1, src.key.k2, src.value.f1, src.value.f2);";
    assertEquals(expectedQuery, flattenedQuery(false, false, true));
  }

  @Test
  public void testFlattenedUpsertQueryWithOnlyKeyColumnsSkipsUpdate() {
    // Every value field is a key field (matched case-insensitively), so there is nothing to update
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE))
        .thenReturn(
            flattenedIntermediateSchema(
                FLATTENED_KEY_FIELDS,
                Arrays.asList(
                    Field.of("K1", LegacySQLTypeName.STRING),
                    Field.of(
                        "k2",
                        LegacySQLTypeName.RECORD,
                        Field.of("nested_k", LegacySQLTypeName.INTEGER)))));
    String query =
        flattenedMergeQueries(false, true, false)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);

    assertFalse(query.contains("WHEN MATCHED"), query);
    assertTrue(
        query.endsWith(
            "WHEN NOT MATCHED THEN INSERT (`k1`, `k2`) VALUES (src.key.k1, src.key.k2);"),
        query);
  }

  @Test
  public void testFlattenedQueryRejectsKeyValueTypeMismatch() {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE))
        .thenReturn(
            flattenedIntermediateSchema(
                FLATTENED_KEY_FIELDS, Arrays.asList(Field.of("k1", LegacySQLTypeName.INTEGER))));
    MergeQueries mergeQueries = flattenedMergeQueries(false, true, false);

    ConnectException e =
        assertThrows(
            ConnectException.class,
            () ->
                mergeQueries.mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER));
    assertEquals(
        "Key field 'k1' has type STRING but the value field 'k1' has type INTEGER; they map to "
            + "the same destination column, so their types must match",
        e.getMessage());
  }

  @Test
  public void testFlattenedQueryRejectsNestedKeyValueTypeMismatch() {
    // Struct equality is positional, so differently ordered subfields do not compare
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE))
        .thenReturn(
            flattenedIntermediateSchema(
                Arrays.asList(
                    Field.of(
                        "k",
                        LegacySQLTypeName.RECORD,
                        Field.of("a", LegacySQLTypeName.STRING),
                        Field.of("b", LegacySQLTypeName.STRING))),
                Arrays.asList(
                    Field.of(
                        "k",
                        LegacySQLTypeName.RECORD,
                        Field.of("b", LegacySQLTypeName.STRING),
                        Field.of("a", LegacySQLTypeName.STRING)))));
    MergeQueries mergeQueries = flattenedMergeQueries(false, true, false);

    assertThrows(
        ConnectException.class,
        () -> mergeQueries.mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER));
  }

  @Test
  public void testFlattenedQueryAllowsKeyValueModeDifference() {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE))
        .thenReturn(
            flattenedIntermediateSchema(
                Arrays.asList(
                    Field.newBuilder("k1", LegacySQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED)
                        .build()),
                Arrays.asList(
                    Field.newBuilder("k1", LegacySQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                    Field.of("f1", LegacySQLTypeName.STRING))));

    String query =
        flattenedMergeQueries(false, true, false)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertTrue(query.contains("ON dstTableAlias.`k1`=src.key.k1 WHEN MATCHED"), query);
  }

  @Test
  public void testFlattenedQueryRejectsRepeatedKeyValueMismatch() {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE))
        .thenReturn(
            flattenedIntermediateSchema(
                Arrays.asList(Field.of("k1", LegacySQLTypeName.STRING)),
                Arrays.asList(
                    Field.newBuilder("k1", LegacySQLTypeName.STRING)
                        .setMode(Field.Mode.REPEATED)
                        .build())));
    MergeQueries mergeQueries = flattenedMergeQueries(false, true, false);

    assertThrows(
        ConnectException.class,
        () -> mergeQueries.mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER));
  }

  private void initialiseMergeBatches() {
    mergeBatches = new MergeBatches("_tmp_6_uuid_epoch");
    mergeBatches.intermediateTableFor(DESTINATION_TABLE);
  }

  @Test
  public void testUpsertQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " dstTableAlias "
            + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM "
            + table(INTERMEDIATE_TABLE)
            + " x "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
            + "ON dstTableAlias."
            + KEY
            + "=src.key "
            + "WHEN MATCHED "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
            + "WHEN NOT MATCHED "
            + "THEN INSERT (`"
            + KEY
            + "`, "
            + "_PARTITIONTIME, "
            + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
            + "src.key, "
            + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
            + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery =
        mergeQueries(true, true, false)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testUpsertQueryWithoutPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " dstTableAlias "
            + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM "
            + table(INTERMEDIATE_TABLE)
            + " x "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
            + "ON dstTableAlias."
            + KEY
            + "=src.key "
            + "WHEN MATCHED "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
            + "WHEN NOT MATCHED "
            + "THEN INSERT (`"
            + KEY
            + "`, "
            + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
            + "src.key, "
            + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery =
        mergeQueries(false, true, false)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testDeleteQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " "
            + "USING ("
            + "SELECT batch.key AS key, partitionTime, value "
            + "FROM ("
            + "SELECT src.i, src.key FROM ("
            + "SELECT ARRAY_AGG("
            + "x ORDER BY i DESC LIMIT 1"
            + ")[OFFSET(0)] src "
            + "FROM ("
            + "SELECT * FROM "
            + table(INTERMEDIATE_TABLE)
            + " "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + ") x "
            + "WHERE x.value IS NULL "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) AS deletes "
            + "RIGHT JOIN ("
            + "SELECT * FROM "
            + table(INTERMEDIATE_TABLE)
            + " "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + ") AS batch "
            + "USING (key) "
            + "WHERE deletes.i IS NULL OR batch.i >= deletes.i "
            + "ORDER BY batch.i ASC) AS src "
            + "ON `"
            + DESTINATION_TABLE.getTable()
            + "`."
            + KEY
            + "=src.key AND src.value IS NULL "
            + "WHEN MATCHED "
            + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`"
            + KEY
            + "`, "
            + "_PARTITIONTIME, "
            + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
            + "src.key, "
            + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
            + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery =
        mergeQueries(true, false, true)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testDeleteQueryWithoutPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " "
            + "USING ("
            + "SELECT batch.key AS key, value "
            + "FROM ("
            + "SELECT src.i, src.key FROM ("
            + "SELECT ARRAY_AGG("
            + "x ORDER BY i DESC LIMIT 1"
            + ")[OFFSET(0)] src "
            + "FROM ("
            + "SELECT * FROM "
            + table(INTERMEDIATE_TABLE)
            + " "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + ") x "
            + "WHERE x.value IS NULL "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) AS deletes "
            + "RIGHT JOIN ("
            + "SELECT * FROM "
            + table(INTERMEDIATE_TABLE)
            + " "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + ") AS batch "
            + "USING (key) "
            + "WHERE deletes.i IS NULL OR batch.i >= deletes.i "
            + "ORDER BY batch.i ASC) AS src "
            + "ON `"
            + DESTINATION_TABLE.getTable()
            + "`."
            + KEY
            + "=src.key AND src.value IS NULL "
            + "WHEN MATCHED "
            + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`"
            + KEY
            + "`, "
            + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
            + "src.key, "
            + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery =
        mergeQueries(false, false, true)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testUpsertDeleteQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " dstTableAlias "
            + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM "
            + table(INTERMEDIATE_TABLE)
            + " x "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
            + "ON dstTableAlias."
            + KEY
            + "=src.key "
            + "WHEN MATCHED AND src.value IS NOT NULL "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
            + "WHEN MATCHED AND src.value IS NULL "
            + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`"
            + KEY
            + "`, "
            + "_PARTITIONTIME, "
            + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
            + "src.key, "
            + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
            + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery =
        mergeQueries(true, true, true)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testUpsertDeleteQueryWithoutPartitionTime() {
    String expectedQuery =
        "MERGE "
            + table(DESTINATION_TABLE)
            + " dstTableAlias "
            + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM "
            + table(INTERMEDIATE_TABLE)
            + " x "
            + "WHERE batchNumber="
            + BATCH_NUMBER
            + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
            + "ON dstTableAlias."
            + KEY
            + "=src.key "
            + "WHEN MATCHED AND src.value IS NOT NULL "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
            + "WHEN MATCHED AND src.value IS NULL "
            + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`"
            + KEY
            + "`, "
            + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
            + "src.key, "
            + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery =
        mergeQueries(false, true, true)
            .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testBatchClearQuery() {
    String expectedQuery =
        "DELETE FROM "
            + table(INTERMEDIATE_TABLE)
            + " WHERE batchNumber <= "
            + BATCH_NUMBER
            + " AND _PARTITIONTIME IS NOT NULL;";
    // No difference in batch clearing between upsert, delete, and both, or with or without
    // partition time
    String actualQuery = MergeQueries.batchClearQuery(INTERMEDIATE_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testNoEmptyBatchCreation() {
    initialiseMergeBatches();

    mergeQueries(false, true, true).mergeFlush(INTERMEDIATE_TABLE);

    assertEquals(0, mergeBatches.incrementBatch(INTERMEDIATE_TABLE));
  }

  @Test
  public void testBatchCreation() {
    initialiseMergeBatches();

    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());
    mergeQueries(false, true, true).mergeFlush(INTERMEDIATE_TABLE);

    assertEquals(1, mergeBatches.incrementBatch(INTERMEDIATE_TABLE));
  }

  @Test
  public void testBigQueryJobInternalErrorRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError jobInternalError =
        new BigQueryError(
            "jobInternalError",
            null,
            "The job encountered an internal error during execution and was unable to complete successfully.");
    when(bigQuery.query(any(QueryJobConfiguration.class)))
        .thenThrow(new BigQueryException(400, "mock job internal error", jobInternalError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              latch.countDown();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    MergeQueries mergeQueries = spy(mergeQueries(false, true, true));

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testBigQueryJobBackendErrorRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError jobBackendError =
        new BigQueryError(
            "jobBackendError",
            null,
            "The job encountered an error during execution. Retrying the job may solve the problem.");
    when(bigQuery.query(any(QueryJobConfiguration.class)))
        .thenThrow(new BigQueryException(400, "mock job backend error", jobBackendError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              latch.countDown();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    MergeQueries mergeQueries = spy(mergeQueries(false, true, true));

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testBigQueryJobRateLimitExceededRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError jobRateLimitExceededError =
        new BigQueryError(
            "jobRateLimitExceeded",
            null,
            "Job exceeded rate limits: Your project_and_region exceeded quota for concurrent queries that append to or overwrite a table.");
    when(bigQuery.query(any(QueryJobConfiguration.class)))
        .thenThrow(
            new BigQueryException(400, "mock job rate limit exceeded", jobRateLimitExceededError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              latch.countDown();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    MergeQueries mergeQueries = spy(mergeQueries(false, true, true));

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testBigQueryTableUnavailableRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError tableUnavailableError =
        new BigQueryError(
            "tableUnavailable",
            null,
            "The table my-project:my_dataset.my_table is currently unavailable. Retry the operation.");
    when(bigQuery.query(any(QueryJobConfiguration.class)))
        .thenThrow(new BigQueryException(400, "mock table unavailable", tableUnavailableError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              latch.countDown();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    MergeQueries mergeQueries = spy(mergeQueries(false, true, true));

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testBigQueryInvalidQueryErrorRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError jobInternalError =
        new BigQueryError(
            "invalidQuery",
            null,
            "Could not serialize access to table my_table due to concurrent update");
    when(bigQuery.query(any(QueryJobConfiguration.class)))
        .thenThrow(new BigQueryException(400, "mock invalid query", jobInternalError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              latch.countDown();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    MergeQueries mergeQueries = mergeQueries(false, true, true);

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any(QueryJobConfiguration.class));
  }

  @Test
  public void testBigQueryRetryExceeded() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    BigQueryError jobInternalError =
        new BigQueryError(
            "invalidQuery",
            null,
            "Could not serialize access to table my_table due to concurrent update");
    when(bigQuery.query(any(QueryJobConfiguration.class)))
        .thenThrow(new BigQueryException(400, "mock invalid query", jobInternalError));
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              latch.countDown();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    MergeQueries mergeQueries = mergeQueries(false, true, true);

    assertThrows(BigQueryConnectException.class, () -> mergeQueries.mergeFlush(INTERMEDIATE_TABLE));
    assertNotEquals(0, latch.getCount());
  }

  private String table(TableId table) {
    return String.format("`%s`.`%s`", table.getDataset(), table.getTable());
  }
}
