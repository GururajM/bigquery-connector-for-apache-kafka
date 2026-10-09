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

import static com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiWriter.DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.ConvertedRecord;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** Task lifecycle tests for upsert/delete through the Storage Write API in MERGE mode. */
public class BigQueryStorageApiMergeSinkTaskTest {
  private static final SinkTaskPropertiesFactory propertiesFactory =
      new SinkTaskPropertiesFactory();
  private static final String TOPIC = "test_topic";
  private static final long WRITE_TIMEOUT_MS = 5_000;

  private final StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream =
      mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
  private final BigQuery bigQuery = mock(BigQuery.class);
  private final Storage storage = mock(Storage.class);
  private final SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
  private final SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
  private final SchemaManager schemaManager = mock(SchemaManager.class);
  private final Time time = new MockTime();
  private final StorageApiBatchModeHandler storageApiBatchHandler =
      mock(StorageApiBatchModeHandler.class);
  private final BigQuerySinkTask testTask =
      new BigQuerySinkTask(
          bigQuery,
          schemaRetriever,
          storage,
          schemaManager,
          mockedStorageWriteApiDefaultStream,
          storageApiBatchHandler,
          time);

  private Map<String, String> properties;

  @BeforeEach
  public void setUp() {
    properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, TOPIC);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    properties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    properties.put(
        BigQuerySinkConfig.STORAGE_WRITE_API_UPSERT_MODE_CONFIG,
        BigQuerySinkConfig.StorageWriteApiUpsertMode.MERGE.name());

    doNothing()
        .when(mockedStorageWriteApiDefaultStream)
        .initializeAndWriteRecords(any(PartitionedTableId.class), anyList(), eq(DEFAULT));
    doNothing().when(mockedStorageWriteApiDefaultStream).shutdown();

    testTask.initialize(sinkTaskContext);
  }

  @AfterEach
  public void tearDown() {
    if (testTask.isRunning()) {
      testTask.stop();
    }
  }

  @Test
  public void testPreCommitReturnsOnlyMergedOffsets() {
    testTask.start(properties);

    Map<TopicPartition, OffsetAndMetadata> offsets =
        Collections.singletonMap(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(5));

    // Nothing has been merged yet, so none of the offsets handed in are safe to commit
    assertTrue(testTask.preCommit(offsets).isEmpty());
  }

  @Test
  public void testFlushIsUnsupported() {
    testTask.start(properties);

    assertThrows(ConnectException.class, () -> testTask.flush(Collections.emptyMap()));
  }

  @Test
  public void testStartSchedulesMergeFlushOnItsOwnExecutor() {
    testTask.start(properties);

    assertNotNull(testTask.mergeFlushExecutor);
    assertFalse(testTask.mergeFlushExecutor.isShutdown());
    // The Storage Write API executor must survive alongside it rather than being overwritten
    assertNotNull(testTask.loadExecutor);
    assertFalse(testTask.loadExecutor.isShutdown());
    assertNotSame(testTask.loadExecutor, testTask.mergeFlushExecutor);
  }

  @Test
  public void testStartWithoutMergeInterval() {
    properties.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "-1");
    properties.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, "10");
    testTask.start(properties);

    assertNull(testTask.mergeFlushExecutor);
  }

  @Test
  public void testStopShutsDownWriterAndExecutors() {
    testTask.start(properties);

    testTask.stop();

    verify(mockedStorageWriteApiDefaultStream, times(1)).shutdown();
    assertTrue(testTask.mergeFlushExecutor.isShutdown());
    assertTrue(testTask.loadExecutor.isShutdown());
  }

  @Test
  public void testPutRoutesToIntermediateTable() {
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord()));

    TableId written = captureWrittenTable().getFullTableId();
    assertEquals("scratch", written.getDataset());
    // Default intermediate table suffix is "tmp", followed by the task ID
    assertTrue(
        written.getTable().startsWith(TOPIC + "_tmp_1_"),
        "Expected the intermediate table, got " + written.getTable());
  }

  @Test
  public void testPutWritesStagingRows() {
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord()));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ConvertedRecord>> records = ArgumentCaptor.forClass(List.class);
    verify(mockedStorageWriteApiDefaultStream, timeout(WRITE_TIMEOUT_MS))
        .initializeAndWriteRecords(any(PartitionedTableId.class), records.capture(), eq(DEFAULT));
    JSONObject row = records.getValue().get(0).converted();
    assertEquals("{\"id\":1}", row.get(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME).toString());
    assertTrue(row.has(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME));
    assertEquals(0, row.get(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD));
  }

  @Test
  public void testStopDeletesIntermediateTablesAfterWriterShutdown() {
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord()));
    TableId intermediateTable = captureWrittenTable().getBaseTableId();

    testTask.stop();

    // Dropping the intermediate table while its streams are still open would fail their appends
    InOrder inOrder = inOrder(mockedStorageWriteApiDefaultStream, bigQuery);
    inOrder.verify(mockedStorageWriteApiDefaultStream).shutdown();
    inOrder.verify(bigQuery).delete(intermediateTable);
  }

  /** Waits for the asynchronous Storage Write API write and returns the table it targeted. */
  private PartitionedTableId captureWrittenTable() {
    ArgumentCaptor<PartitionedTableId> table = ArgumentCaptor.forClass(PartitionedTableId.class);
    verify(mockedStorageWriteApiDefaultStream, timeout(WRITE_TIMEOUT_MS))
        .initializeAndWriteRecords(table.capture(), anyList(), eq(DEFAULT));
    return table.getValue();
  }

  private SinkRecord spoofSinkRecord() {
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
    Struct key = new Struct(keySchema).put("id", 1L);

    Schema valueSchema =
        SchemaBuilder.struct().field("sink_task_test_field", Schema.STRING_SCHEMA).build();
    Struct value = new Struct(valueSchema).put("sink_task_test_field", "sink task test row");

    return new SinkRecord(
        TOPIC, 0, keySchema, key, valueSchema, value, 0, null, TimestampType.NO_TIMESTAMP_TYPE);
  }
}
