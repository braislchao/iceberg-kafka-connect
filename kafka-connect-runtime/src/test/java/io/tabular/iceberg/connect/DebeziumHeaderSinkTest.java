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
package io.tabular.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.data.RecordConverter;
import io.tabular.iceberg.connect.data.SchemaUpdate;
import io.tabular.iceberg.connect.data.SchemaUpdate.AddColumn;
import io.tabular.iceberg.connect.transforms.DebeziumTransform;
import java.util.Collection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * Chains the real {@link DebeziumTransform} into the real {@link RecordConverter} to prove the two
 * halves fit together: the transform copies a header into the CDC metadata, and the sink adds it as
 * a field <em>inside</em> the existing {@code _cdc} struct rather than as a top-level column.
 */
public class DebeziumHeaderSinkTest {

  private static final String HEADER = "cell_id";
  private static final String HEADER_VALUE = "cell-two";

  private static final org.apache.kafka.connect.data.Schema ROW_SCHEMA =
      SchemaBuilder.struct()
          .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("data", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .build();

  private static final org.apache.kafka.connect.data.Schema SOURCE_SCHEMA =
      SchemaBuilder.struct()
          .field("db", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("table", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .build();

  private static final org.apache.kafka.connect.data.Schema ENVELOPE_SCHEMA =
      SchemaBuilder.struct()
          .field("op", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("source", SOURCE_SCHEMA)
          .field("before", ROW_SCHEMA)
          .field("after", ROW_SCHEMA)
          .build();

  /** An existing CDC table: a _cdc struct with no header field. */
  private static final Schema TABLE_SCHEMA_BEFORE =
      new Schema(
          NestedField.optional(1, "id", Types.LongType.get()),
          NestedField.optional(2, "data", Types.StringType.get()),
          NestedField.optional(
              3,
              "_cdc",
              Types.StructType.of(
                  NestedField.optional(4, "op", Types.StringType.get()),
                  NestedField.optional(5, "ts", Types.TimestampType.withZone()),
                  NestedField.optional(6, "offset", Types.LongType.get()),
                  NestedField.optional(7, "source", Types.StringType.get()),
                  NestedField.optional(8, "target", Types.StringType.get()))));

  /** The same table after the sink has evolved it. */
  private static final Schema TABLE_SCHEMA_AFTER =
      new Schema(
          NestedField.optional(1, "id", Types.LongType.get()),
          NestedField.optional(2, "data", Types.StringType.get()),
          NestedField.optional(
              3,
              "_cdc",
              Types.StructType.of(
                  NestedField.optional(4, "op", Types.StringType.get()),
                  NestedField.optional(5, "ts", Types.TimestampType.withZone()),
                  NestedField.optional(6, "offset", Types.LongType.get()),
                  NestedField.optional(7, "source", Types.StringType.get()),
                  NestedField.optional(8, "target", Types.StringType.get()),
                  NestedField.optional(9, "cell_id", Types.StringType.get()))));

  @Test
  public void testHeaderFieldIsAddedInsideTheCdcStruct() {
    Struct transformed = transform(true);

    Table table = mock(Table.class);
    when(table.schema()).thenReturn(TABLE_SCHEMA_BEFORE);
    RecordConverter converter = new RecordConverter(table, mock(IcebergSinkConfig.class));

    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    converter.convert(transformed, updates);
    Collection<AddColumn> added = updates.addColumns();

    assertThat(added).hasSize(1);
    AddColumn addColumn = added.iterator().next();
    // nested, not top level: this is the whole point of putting it in _cdc
    assertThat(addColumn.parentName()).isEqualTo("_cdc");
    assertThat(addColumn.name()).isEqualTo(HEADER);
    assertThat(addColumn.type()).isEqualTo(Types.StringType.get());
  }

  @Test
  public void testHeaderFieldIsWrittenOnceTheColumnExists() {
    // mirrors IcebergWriter, which re-converts the triggering record against the evolved schema
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(TABLE_SCHEMA_AFTER);
    RecordConverter converter = new RecordConverter(table, mock(IcebergSinkConfig.class));

    Record row = converter.convert(transform(true));
    Record cdc = (Record) row.getField("_cdc");

    assertThat(cdc.getField(HEADER)).isEqualTo(HEADER_VALUE);
    assertThat(cdc.getField("op")).isEqualTo("I");
    assertThat(cdc.getField("source")).isEqualTo("db.tbl");
    assertThat(row.getField("id")).isEqualTo(1L);
  }

  @Test
  public void testRecordWithoutTheHeaderWritesNullAndNoSchemaChange() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(TABLE_SCHEMA_AFTER);
    RecordConverter converter = new RecordConverter(table, mock(IcebergSinkConfig.class));

    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    Record row = converter.convert(transform(false), updates);

    assertThat(updates.empty()).isTrue();
    assertThat(((Record) row.getField("_cdc")).getField(HEADER)).isNull();
  }

  private Struct transform(boolean withHeader) {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.headers", HEADER));

      Struct row = new Struct(ROW_SCHEMA).put("id", 1L).put("data", "payload");
      Struct source = new Struct(SOURCE_SCHEMA).put("db", "db").put("table", "tbl");
      Struct envelope =
          new Struct(ENVELOPE_SCHEMA)
              .put("op", "c")
              .put("ts_ms", System.currentTimeMillis())
              .put("source", source)
              .put("before", row)
              .put("after", row);

      SinkRecord record = new SinkRecord("topic", 0, null, null, ENVELOPE_SCHEMA, envelope, 0);
      if (withHeader) {
        record.headers().addString(HEADER, HEADER_VALUE);
      }
      return (Struct) smt.apply(record).value();
    }
  }
}
