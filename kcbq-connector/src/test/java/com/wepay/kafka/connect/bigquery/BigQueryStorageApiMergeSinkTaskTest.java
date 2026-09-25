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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Task lifecycle tests for upsert/delete through the Storage Write API in MERGE mode. */
public class BigQueryStorageApiMergeSinkTaskTest {
  private static final SinkTaskPropertiesFactory propertiesFactory =
      new SinkTaskPropertiesFactory();
  private static final String TOPIC = "test_topic";

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
}
