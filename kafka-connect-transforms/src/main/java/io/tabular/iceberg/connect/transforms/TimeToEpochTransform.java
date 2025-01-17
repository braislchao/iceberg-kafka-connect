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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Map;

public class TimeToEpochTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("field.strategy", ConfigDef.Type.STRING, "value", ConfigDef.Importance.HIGH,
                    "Specify whether to process `key`, `value`, or `both` fields.");

    private String fieldStrategy;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldStrategy = config.getString("field.strategy").toLowerCase();
    }

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        Schema updatedKeySchema = null;
        Struct updatedKeyStruct = null;
        if ("key".equals(fieldStrategy) || "both".equals(fieldStrategy)) {
            if (record.keySchema() != null && record.key() instanceof Struct) {
                updatedKeySchema = processSchema(record.keySchema());
                updatedKeyStruct = processStruct((Struct) record.key(), updatedKeySchema);
            }
        }

        Schema updatedValueSchema = null;
        Struct updatedValueStruct = null;
        if ("value".equals(fieldStrategy) || "both".equals(fieldStrategy)) {
            if (record.valueSchema() != null && record.value() instanceof Struct) {
                updatedValueSchema = processSchema(record.valueSchema());
                updatedValueStruct = processStruct((Struct) record.value(), updatedValueSchema);
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedKeySchema != null ? updatedKeySchema : record.keySchema(),
                updatedKeyStruct != null ? updatedKeyStruct : record.key(),
                updatedValueSchema != null ? updatedValueSchema : record.valueSchema(),
                updatedValueStruct != null ? updatedValueStruct : record.value(),
                record.timestamp(),
                record.headers()
        );
    }

    private Schema processSchema(Schema originalSchema) {
        SchemaBuilder builder = SchemaBuilder.struct().name(originalSchema.name());
        for (Field field : originalSchema.fields()) {
            if (Time.LOGICAL_NAME.equals(field.schema().name())) {
                builder.field(field.name(), Schema.INT64_SCHEMA); // TIME fields converted to TIMESTAMP
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    private Struct processStruct(Struct originalStruct, Schema updatedSchema) {
        Struct updatedStruct = new Struct(updatedSchema);
        for (Field field : updatedSchema.fields()) {
            Object fieldValue = originalStruct.get(field.name());
            if (field.schema().name() != null && field.schema().name().equals(Time.LOGICAL_NAME)) {
                // Convert TIME to TIMESTAMP in epoch milliseconds
                LocalTime time = LocalTime.ofSecondOfDay(((java.util.Date) fieldValue).getTime() / 1000);
                LocalDateTime dateTime = LocalDateTime.of(1970, 1, 1, time.getHour(), time.getMinute(), time.getSecond());
                updatedStruct.put(field.name(), dateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
            } else {
                updatedStruct.put(field.name(), fieldValue);
            }
        }
        return updatedStruct;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

}