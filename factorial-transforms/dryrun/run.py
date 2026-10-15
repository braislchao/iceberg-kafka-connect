"""Start one local connector per replayed topic, mirroring connector-tabular-azure-bronze-events-122001.

Differences from prod, all forced by running locally:
  * one connector per topic, so cdc.offset.start can map local offset 0 onto that topic's
    first replayed prod offset (prod 122001 sets no offset start: _cdc.offset is the raw offset)
  * REST catalog + S3 mock instead of Glue + S3
  * 30 s commit interval instead of 180 s

usage: run.py <topic> <first prod offset> [<topic> <first prod offset> ...]
"""
import json
import sys
import urllib.request

SR = "https://psrc-6k76o2.germanywestcentral.azure.confluent.cloud"
SR_AUTH = "${file:/secrets/sr.properties:auth}"

base = {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": SR,
    "key.converter.basic.auth.credentials.source": "USER_INFO",
    "key.converter.basic.auth.user.info": SR_AUTH,
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": SR,
    "value.converter.basic.auth.credentials.source": "USER_INFO",
    "value.converter.basic.auth.user.info": SR_AUTH,
    "transforms": "dbztransform,insertTS",
    "transforms.dbztransform.type": "co.factorial.iceberg.connect.transforms.DebeziumTransform",
    "transforms.dbztransform.cdc.target.pattern": "data_lake_bronze.{table}",
    "transforms.dbztransform.cdc.headers": "cell_id",
    "transforms.insertTS.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.insertTS.timestamp.field": "_event_ts",
    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.tables.route-field": "_cdc.target",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.tables.schema-force-optional": "true",
    "iceberg.tables.default-partition-by": "day(_event_ts)",
    "iceberg.control.commit.interval-ms": "30000",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://rest:8181",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://s3:9090",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.catalog.client.region": "eu-central-1",
    "iceberg.kafka.bootstrap.servers": "kafka:29092",
}

args = sys.argv[1:]
for topic, first in zip(args[::2], args[1::2]):
    name = "dryrun-" + topic.rsplit(".", 1)[1].replace("_", "-")
    cfg = {
        **base,
        "name": name,
        "topics": topic,
        "transforms.dbztransform.cdc.offset.start": first,
        "iceberg.control.topic": "control-" + name,
    }
    req = urllib.request.Request(
        f"http://localhost:8083/connectors/{name}/config",
        data=json.dumps(cfg).encode(),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req).read()
    print("started", name, "offset.start", first)
