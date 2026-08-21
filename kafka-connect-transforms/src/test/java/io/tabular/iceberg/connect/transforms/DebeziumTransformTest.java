/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.jupiter.api.Test;

public class DebeziumTransformTest {

  private static final Schema KEY_SCHEMA =
      SchemaBuilder.struct().field("account_id", Schema.INT64_SCHEMA).build();

  private static final Schema ROW_SCHEMA =
      SchemaBuilder.struct()
          .field("account_id", Schema.INT64_SCHEMA)
          .field("balance", Decimal.schema(2))
          .field("last_updated", Schema.STRING_SCHEMA)
          .build();

  private static final Schema SOURCE_SCHEMA =
      SchemaBuilder.struct()
          .field("db", Schema.STRING_SCHEMA)
          .field("schema", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .build();

  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("op", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("source", SOURCE_SCHEMA)
          .field("before", ROW_SCHEMA)
          .field("after", ROW_SCHEMA)
          .build();

  @Test
  public void testDmsTransformNull() {
    try (DmsTransform<SinkRecord> smt = new DmsTransform<>()) {
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isNull();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemaless() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Map<String, Object> event = createDebeziumEventMap("u");
      Map<String, Object> key = ImmutableMap.of("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, null, key, null, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<String, Object> value = (Map<String, Object>) result.value();

      assertThat(value.get("account_id")).isEqualTo(1);

      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("offset")).isEqualTo(0L);
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Map.class);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemalessWithOffsetStart() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(
          ImmutableMap.of(
              "cdc.target.pattern", "{db}_x.{table}_x",
              "cdc.offset.start", 100000000L));

      Map<String, Object> event = createDebeziumEventMap("u");
      Map<String, Object> key = ImmutableMap.of("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, null, key, null, event, 5);

      SinkRecord result = smt.apply(record);
      Map<String, Object> value = (Map<String, Object>) result.value();
      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("offset")).isEqualTo(100000005L);
    }
  }

  @Test
  public void testDebeziumTransformWithSchema() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Struct event = createDebeziumEventStruct("u");
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();

      assertThat(value.get("account_id")).isEqualTo(1L);

      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("offset")).isEqualTo(0L);
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);
    }
  }

  @Test
  public void testDebeziumTransformWithSchemaAndOffsetStart() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(
          ImmutableMap.of(
              "cdc.target.pattern", "{db}_x.{table}_x",
              "cdc.offset.start", 100000000L));

      Struct event = createDebeziumEventStruct("u");
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 5);

      SinkRecord result = smt.apply(record);
      Struct value = (Struct) result.value();
      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("offset")).isEqualTo(100000005L);
    }
  }

  private static final String HEADER = "cell_id";
  private static final String HEADER_VALUE = "cell-one";

  @Test
  public void testDebeziumTransformCopiesHeaderIntoCdcMetadata() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(
          ImmutableMap.of(
              "cdc.target.pattern", "{db}_x.{table}_x",
              "cdc.headers", HEADER));

      Struct event = createDebeziumEventStruct("u");
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 0);
      record.headers().addString(HEADER, HEADER_VALUE);

      Struct cdcMetadata = ((Struct) smt.apply(record).value()).getStruct("_cdc");

      assertThat(cdcMetadata.schema().field(HEADER).schema().isOptional()).isTrue();
      assertThat(cdcMetadata.get(HEADER)).isEqualTo(HEADER_VALUE);
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);
    }
  }

  @Test
  public void testDebeziumTransformCopiesSeveralHeaders() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.headers", "cell_id,tenant"));

      Struct event = createDebeziumEventStruct("u");
      SinkRecord record = new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, event, 0);
      record.headers().addString("cell_id", HEADER_VALUE);
      record.headers().addString("tenant", "acme");

      Struct cdcMetadata = ((Struct) smt.apply(record).value()).getStruct("_cdc");

      assertThat(cdcMetadata.get("cell_id")).isEqualTo(HEADER_VALUE);
      assertThat(cdcMetadata.get("tenant")).isEqualTo("acme");
    }
  }

  @Test
  public void testDebeziumTransformMissingHeaderKeepsStableSchema() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.headers", HEADER));

      Struct event = createDebeziumEventStruct("u");
      SinkRecord withHeader = new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, event, 0);
      withHeader.headers().addString(HEADER, HEADER_VALUE);
      SinkRecord without = new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, event, 1);

      Struct withCdc = ((Struct) smt.apply(withHeader).value()).getStruct("_cdc");
      Struct withoutCdc = ((Struct) smt.apply(without).value()).getStruct("_cdc");

      // a missing header must not change the schema, or the sink would evolve it back and forth
      assertThat(withoutCdc.schema()).isEqualTo(withCdc.schema());
      assertThat(withoutCdc.get(HEADER)).isNull();
    }
  }

  @Test
  public void testDebeziumTransformNoHeadersConfigured() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Struct event = createDebeziumEventStruct("u");
      SinkRecord record = new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, event, 0);
      record.headers().addString(HEADER, HEADER_VALUE);

      Struct cdcMetadata = ((Struct) smt.apply(record).value()).getStruct("_cdc");

      // existing tables must stay untouched until a connector opts in
      assertThat(cdcMetadata.schema().field(HEADER)).isNull();
    }
  }

  @Test
  public void testDebeziumTransformRejectsReservedFieldName() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      assertThatThrownBy(() -> smt.configure(ImmutableMap.of("cdc.headers", "op")))
          .isInstanceOf(ConfigException.class);
    }
  }

  @Test
  public void testDebeziumTransformRejectsDuplicateHeader() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      assertThatThrownBy(
              () -> smt.configure(ImmutableMap.of("cdc.headers", "cell_id,cell_id")))
          .isInstanceOf(ConfigException.class);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemalessCopiesHeader() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.headers", HEADER));

      Map<String, Object> event = createDebeziumEventMap("u");
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, event, 0);
      record.headers().addString(HEADER, HEADER_VALUE);

      Map<String, Object> value = (Map<String, Object>) smt.apply(record).value();
      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");

      assertThat(cdcMetadata.get(HEADER)).isEqualTo(HEADER_VALUE);
    }
  }

  @Test
  public void testDebeziumTransformHeaderFromWireBytes() {
    SimpleHeaderConverter converter = new SimpleHeaderConverter();
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.headers", HEADER));

      byte[] onTheWire = HEADER_VALUE.getBytes(StandardCharsets.UTF_8);
      assertThat(converter.fromConnectHeader("topic", HEADER, Schema.STRING_SCHEMA, HEADER_VALUE))
          .isEqualTo(onTheWire);

      ConnectHeaders headers = new ConnectHeaders();
      headers.add(HEADER, converter.toConnectHeader("topic", HEADER, onTheWire));

      Struct event = createDebeziumEventStruct("u");
      SinkRecord record =
          new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, event, 0, null, null, headers);

      Struct cdcMetadata = ((Struct) smt.apply(record).value()).getStruct("_cdc");
      assertThat(cdcMetadata.get(HEADER)).isEqualTo(HEADER_VALUE);
    }
  }

  @Test
  public void testDebeziumTransformRawByteHeader() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.headers", HEADER));

      Struct event = createDebeziumEventStruct("u");
      SinkRecord record = new SinkRecord("topic", 0, null, null, VALUE_SCHEMA, event, 0);
      record.headers().addBytes(HEADER, HEADER_VALUE.getBytes(StandardCharsets.UTF_8));

      Struct cdcMetadata = ((Struct) smt.apply(record).value()).getStruct("_cdc");
      assertThat(cdcMetadata.get(HEADER)).isEqualTo(HEADER_VALUE);
    }
  }

  private Map<String, Object> createDebeziumEventMap(String operation) {
    Map<String, Object> source =
        ImmutableMap.of(
            "db", "db",
            "schema", "schema",
            "table", "tbl");

    Map<String, Object> data =
        ImmutableMap.of(
            "account_id", 1,
            "balance", 100,
            "last_updated", Instant.now().toString());

    return ImmutableMap.of(
        "op", operation,
        "ts_ms", System.currentTimeMillis(),
        "source", source,
        "before", data,
        "after", data);
  }

  private Struct createDebeziumEventStruct(String operation) {
    Struct source =
        new Struct(SOURCE_SCHEMA).put("db", "db").put("schema", "schema").put("table", "tbl");

    Struct data =
        new Struct(ROW_SCHEMA)
            .put("account_id", 1L)
            .put("balance", BigDecimal.valueOf(100))
            .put("last_updated", Instant.now().toString());

    return new Struct(VALUE_SCHEMA)
        .put("op", operation)
        .put("ts_ms", System.currentTimeMillis())
        .put("source", source)
        .put("before", data)
        .put("after", data);
  }
}
