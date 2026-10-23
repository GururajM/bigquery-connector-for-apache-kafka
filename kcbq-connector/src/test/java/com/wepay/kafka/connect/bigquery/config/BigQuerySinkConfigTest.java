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

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONVERT_DEBEZIUM_DECIMAL_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DEBEZIUM_VARIABLE_SCALE_DECIMAL_HANDLING_MODE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DECIMAL_HANDLING_MODE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.STORAGE_WRITE_API_UPSERT_MODE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class BigQuerySinkConfigTest {
  private SinkPropertiesFactory propertiesFactory;

  @BeforeEach
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  // Just to ensure that the basic properties don't cause any exceptions on any public methods
  @Test
  public void metaTestBasicConfigProperties() {
    Map<String, String> basicConfigProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(basicConfigProperties);
    config.getList(BigQuerySinkConfig.TOPICS_CONFIG);
    config.getString(BigQuerySinkConfig.PROJECT_CONFIG);
    config.getKey();
    config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
    config.getInt(BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG);
  }

  @Test
  public void testGetSchemaConverter() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaData");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);

    assertInstanceOf(BigQuerySchemaConverter.class, testConfig.getSchemaConverter());
  }

  @Test
  public void testGetRecordConverter() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaData");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);

    assertInstanceOf(BigQueryRecordConverter.class, testConfig.getRecordConverter());
  }

  @Test
  public void testInvalidAvroCacheSize() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG, "-1");

    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(badConfigProperties));
  }

  /** Test the default for the field name is not present. */
  @Test
  public void testEmptyTimestampPartitionFieldName() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertFalse(testConfig.getTimestampPartitionFieldName().isPresent());
  }

  /** Test the field name being non-empty and the decorator set to false works correctly. */
  @Test
  public void testTimestampPartitionFieldName() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertTrue(testConfig.getTimestampPartitionFieldName().isPresent());
    assertFalse(testConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG));
  }

  /** Test the default for the field names is not present. */
  @Test
  public void testEmptyClusteringFieldNames() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertFalse(testConfig.getClusteringPartitionFieldNames().isPresent());
  }

  /** Test if the field names are more than four fields errors correctly. */
  @Test
  public void testClusteringPartitionFieldNamesWithMoreThanFourFieldsError() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
        "column1,column2,column3,column4,column5");

    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  /** Test the field names being non-empty and the partitioning field exists works correctly. */
  @Test
  public void testClusteringFieldNames() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    configProperties.put(
        BigQuerySinkConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG, "column1,column2");

    ArrayList<String> expectedClusteringPartitionFieldName =
        new ArrayList<>(Arrays.asList("column1", "column2"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Optional<List<String>> testClusteringPartitionFieldName =
        testConfig.getClusteringPartitionFieldNames();
    assertTrue(testClusteringPartitionFieldName.isPresent());
    assertEquals(expectedClusteringPartitionFieldName, testClusteringPartitionFieldName.get());
  }

  /** Test the default for the partition expiration is not present. */
  @Test
  public void testEmptyPartitionExpirationMs() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertFalse(testConfig.getPartitionExpirationMs().isPresent());
  }

  /** Test the partition expiration is set correctly for a valid value. */
  @Test
  public void testValidPartitionExpirationMs() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_EXPIRATION_CONFIG, "1");
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertTrue(testConfig.getPartitionExpirationMs().isPresent());
    assertEquals(Optional.of(1L), testConfig.getPartitionExpirationMs());
  }

  /** Test the partition expiration being non-positive errors correctly. */
  @Test
  public void testMinimumPartitionExpirationMs() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_EXPIRATION_CONFIG, "0");
    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  @Test
  public void testValidTimePartitioningTypes() {
    Map<String, String> configProperties = propertiesFactory.getProperties();

    for (TimePartitioning.Type type : TimePartitioning.Type.values()) {
      configProperties.put(BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG, type.name());
      Optional<TimePartitioning.Type> timePartitioningType =
          new BigQuerySinkConfig(configProperties).getTimePartitioningType();
      assertTrue(timePartitioningType.isPresent());
      assertEquals(type, timePartitioningType.get());
    }

    configProperties.put(
        BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG,
        BigQuerySinkConfig.TIME_PARTITIONING_TYPE_NONE);
    Optional<TimePartitioning.Type> timePartitioningType =
        new BigQuerySinkConfig(configProperties).getTimePartitioningType();
    assertEquals(Optional.empty(), timePartitioningType);
  }

  @Test
  public void testInvalidTimePartitioningType() {
    Map<String, String> configProperties = propertiesFactory.getProperties();

    configProperties.put(BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG, "fortnight");
    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  @Test
  public void testTopic2TableInvalidFormat() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:");
    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  @Test
  public void testTopic2TableDuplicateTopic() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:table, topic:table2");
    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  @Test
  public void testTopic2TableDuplicateTable() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:table, topic2:table");
    new BigQuerySinkConfig(configProperties);
  }

  @Test
  public void testTopic2TableSemicolonOnly() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, ":");
    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  @Test
  public void testValidTopic2TableMap() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:table, topic2:table2");
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, String> topic2TableMap = new HashMap<>();
    topic2TableMap.put("topic", "table");
    topic2TableMap.put("topic2", "table2");
    assertEquals(topic2TableMap, config.getTopic2TableMap().get());
  }

  @Test
  public void testTopic2TableEmptyString() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "");
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertFalse(config.getTopic2TableMap().isPresent());
  }

  @Test
  public void testTopic2TableCommaOnly() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, ",");
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertFalse(config.getTopic2TableMap().isPresent());
  }

  @Test
  public void testInvalidMaxRetries() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(BigQuerySinkConfig.MAX_RETRIES_CONFIG, "-1");

    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(badConfigProperties));
  }

  @Test
  public void testInvalidCommitInterval() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG, "0");

    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(badConfigProperties));
  }

  @Test
  public void testConvertDebeziumVariableScaleDecimal() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(
        BigQuerySinkConfig.DecimalHandlingMode.RECORD,
        config.getVariableScaleDecimalHandlingMode());

    configProperties.put(CONVERT_DEBEZIUM_DECIMAL_CONFIG, "true");
    config = new BigQuerySinkConfig(configProperties);
    assertEquals(
        BigQuerySinkConfig.DecimalHandlingMode.NUMERIC,
        config.getVariableScaleDecimalHandlingMode());

    configProperties.put(
        DEBEZIUM_VARIABLE_SCALE_DECIMAL_HANDLING_MODE_CONFIG,
        BigQuerySinkConfig.DecimalHandlingMode.FLOAT.name());
    config = new BigQuerySinkConfig(configProperties);
    assertEquals(
        BigQuerySinkConfig.DecimalHandlingMode.FLOAT, config.getVariableScaleDecimalHandlingMode());
  }

  @ParameterizedTest
  @EnumSource(BigQuerySinkConfig.DecimalHandlingMode.class)
  void testDebeziumVariableScaleDecimalHandlingMode(
      BigQuerySinkConfig.DecimalHandlingMode handlingMode) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(DEBEZIUM_VARIABLE_SCALE_DECIMAL_HANDLING_MODE_CONFIG, handlingMode.name());
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(handlingMode, config.getVariableScaleDecimalHandlingMode());

    configProperties.put(
        DEBEZIUM_VARIABLE_SCALE_DECIMAL_HANDLING_MODE_CONFIG,
        handlingMode.name().toLowerCase(Locale.ROOT));
    config = new BigQuerySinkConfig(configProperties);
    assertEquals(handlingMode, config.getVariableScaleDecimalHandlingMode());
  }

  @ParameterizedTest
  @EnumSource(BigQuerySinkConfig.DecimalHandlingMode.class)
  void testDecimalHandlingMode(BigQuerySinkConfig.DecimalHandlingMode handlingMode) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(DECIMAL_HANDLING_MODE_CONFIG, handlingMode.name());
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(handlingMode, config.getDecimalHandlingMode());

    configProperties.put(
        DECIMAL_HANDLING_MODE_CONFIG, handlingMode.name().toLowerCase(Locale.ROOT));
    config = new BigQuerySinkConfig(configProperties);
    assertEquals(handlingMode, config.getDecimalHandlingMode());
  }

  @Test
  void testDefaults() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);

    assertEquals(
        BigQuerySinkConfig.DecimalHandlingMode.RECORD,
        config.getVariableScaleDecimalHandlingMode());
    assertEquals(BigQuerySinkConfig.DecimalHandlingMode.FLOAT, config.getDecimalHandlingMode());
    assertFalse(config.getShouldConvertDebeziumTimestampToInteger());
    assertFalse(config.getShouldUseAvroTemporalLogicalTypes());
  }

  @Test
  void testUseAvroLogicalTypesConfig() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertFalse(config.getShouldUseAvroTemporalLogicalTypes());

    configProperties.put(BigQuerySinkConfig.USE_AVRO_TEMPORAL_LOGICAL_TYPES_CONFIG, "true");
    config = new BigQuerySinkConfig(configProperties);
    assertTrue(config.getShouldUseAvroTemporalLogicalTypes());
  }

  private Map<String, ConfigValue> errorMap(Config cfg) {
    Map<String, ConfigValue> result = new HashMap<>();
    cfg.configValues().stream()
        .filter(cv -> !cv.errorMessages().isEmpty())
        .forEach(cv -> result.put(cv.name(), cv));
    return result;
  }

  @Test
  void testKafkaKeyFieldWithUpsertAndStorageWriteApi() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, "KeyField");
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.keySet())
        .containsExactlyInAnyOrder(
            BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG,
            BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG,
            "keyfile");
  }

  @Test
  void testKafkaKeyFieldWithDeleteAndStorageWriteApi() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, "KeyField");
    configProperties.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.keySet())
        .containsExactlyInAnyOrder(
            BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG,
            BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG,
            "keyfile",
            BigQuerySinkConfig.DELETE_ENABLED_CONFIG);
  }

  @Test
  void testKafkaKeyFieldWithDeleteAndDefaultWrite() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.keySet())
        .containsExactlyInAnyOrder(
            BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG,
            BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG,
            "keyfile");
  }

  @Test
  void testStorageWriteApiUpsertModeDefaultsToCdc() {
    BigQuerySinkConfig config = new BigQuerySinkConfig(propertiesFactory.getProperties());
    assertEquals(
        BigQuerySinkConfig.StorageWriteApiUpsertMode.CDC, config.getStorageWriteApiUpsertMode());
  }

  @ParameterizedTest
  @CsvSource({"MERGE, MERGE", "merge, MERGE", "Cdc, CDC"})
  void testStorageWriteApiUpsertModeIsCaseInsensitive(
      String value, BigQuerySinkConfig.StorageWriteApiUpsertMode expected) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, value);

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(expected, config.getStorageWriteApiUpsertMode());
  }

  @Test
  void testInvalidStorageWriteApiUpsertMode() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, "APPEND");

    assertThrows(ConfigException.class, () -> new BigQuerySinkConfig(configProperties));
  }

  @ParameterizedTest(name = "upsert={0}, delete={1}")
  @CsvSource({"true,false", "true,true", "false,true"})
  void testStorageWriteApiMergeModePassesValidation(String upsert, String delete) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, upsert);
    configProperties.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, delete);
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, "MERGE");

    // Delete-only is supported, unlike in CDC mode, since the connector runs the MERGE
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.keySet())
        .containsExactlyInAnyOrder(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, "keyfile");
  }

  @Test
  void testStorageWriteApiMergeModeRequiresMergeTrigger() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, "MERGE");
    configProperties.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "-1");
    configProperties.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, "-1");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.get(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG).errorMessages())
        .containsExactly(UpsertDeleteValidator.MERGE_MODE_NO_MERGE_TRIGGER_ERROR);
  }

  @Test
  void testStorageWriteApiCdcModePassesValidation() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, "CDC");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.keySet())
        .containsExactlyInAnyOrder(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, "keyfile");
  }

  @ParameterizedTest(name = "{0} with MERGE")
  @CsvSource({
    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG
        + ","
        + UpsertDeleteValidator.MERGE_MODE_TRACK_PUT_ATTEMPTS_ERROR,
    BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG
        + ","
        + UpsertDeleteValidator.MERGE_MODE_BATCH_MODE_ERROR
  })
  void testStorageWriteApiMergeModeRejectsUnsupportedOption(String option, String error) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, "MERGE");
    configProperties.put(option, "true");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors.get(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG).errorMessages())
        .containsExactly(error);
  }

  @ParameterizedTest(name = "{0} with CDC")
  @ValueSource(
      strings = {
        BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
        BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG
      })
  void testStorageWriteApiCdcModeAcceptsOption(String option) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, "CDC");
    configProperties.put(option, "true");

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, ConfigValue> errors = errorMap(config.validate());
    assertThat(errors).doesNotContainKey(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG);
  }

  @ParameterizedTest(name = "storageWriteApi={0}, upsert={1}, delete={2}, mode={3} -> {4}")
  @CsvSource({
    "true,  true,  false, MERGE, true",
    "true,  false, true,  MERGE, true",
    "true,  true,  true,  MERGE, true",
    "true,  true,  false, CDC,   false",
    "true,  false, false, MERGE, false",
    "false, true,  false, MERGE, false",
    "false, false, true,  MERGE, false"
  })
  void testIsStorageWriteApiMergeMode(
      boolean useStorageWriteApi,
      boolean upsertEnabled,
      boolean deleteEnabled,
      String mode,
      boolean expected) {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(
        BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, String.valueOf(useStorageWriteApi));
    configProperties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, String.valueOf(upsertEnabled));
    configProperties.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, String.valueOf(deleteEnabled));
    configProperties.put(STORAGE_WRITE_API_UPSERT_MODE_CONFIG, mode);

    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(expected, config.isStorageWriteApiMergeMode());
  }
}
