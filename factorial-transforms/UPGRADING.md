# Upgrading the bronze Iceberg sink

How to move the bronze CDC sinks to a new Apache Iceberg release, or to a new Kafka Connect
worker image, without breaking `data_lake_bronze`. Update this guide whenever an upgrade
teaches something new; the [log](#log) at the end records what each upgrade found.

> **Status:** production still runs the Tabular build (`custom-0.6.20-v1`). The Apache path below
> has been verified locally against production data, but not yet deployed. Steps marked
> **(not yet done)** have never been run and need their own plan before anyone runs them.

## What runs, and where each piece comes from

| Piece | Source | Built by |
| --- | --- | --- |
| Sink runtime (Apache Iceberg Kafka Connect) | `apache/iceberg` at tag `apache-iceberg-<version>` | you, from source: the runtime zip is not published |
| Our `DebeziumTransform` (`cdc.offset.start`, `cdc.headers`) | `factorial-transforms/` in this repo | `gradle -p factorial-transforms jar` |
| Connect worker image | `docker/connect-iceberg/` in `factorial-infrastructure` | `docker/connect-iceberg/build.sh`, pushed to ECR |
| Connect cluster and connectors | `k8s/argocdv2/apps/data-mesh/_profile/aws-production/kafka-connect/data/iceberg-sink-data/` | ArgoCD |

The connectors are `connector-tabular-azure-bronze-events-122001` (2 tables),
`connector-tabular-azure-pro-total-211007` (about 1,070 tables) and
`connector-tabular-azure-events-121008` (silver `events_partitioned`).

Our transform is the only code of ours in the sink. It is Apache's `DebeziumTransform` plus two
patches, and its jar goes into the Apache plugin's `lib/` directory so it shares that plugin's
classloader. That is why every dependency in `build.gradle` is `compileOnly`.

## Rules that do not change between versions

- **Never change `cdc.offset.start` on pro-total.** It is `200000000` and keeps pre-cutover
  offsets out of bronze.
- **Upgrade 122001 first, pro-total second.** 122001 covers two tables; pro-total covers almost
  all of bronze.
- **The worker JVM must be at least the Java version Iceberg is compiled for.** If it is older,
  Connect skips the plugin without logging an error. The loader registers, no connector class
  appears, and connectors fail as "class not found".
- **A sink on a topic with no new records can stop committing.** The sink reads commit requests
  only inside `put()`. With no new records Connect calls `put()` once per
  `offset.flush.interval.ms` (60 s by default), which is longer than the 45 s consumer session
  timeout. The commit consumer then never joins its group, and every commit round ends with
  `committed to 0 table(s)`. Production topics never go quiet; the dry-run does, so it lowers
  the interval.

## 1. Read the release notes

From `https://iceberg.apache.org/blog/apache-iceberg-<version>-release/`, note:

- **Kafka Connect changes:** new or renamed `iceberg.*` settings, changed defaults, offset or
  commit behaviour.
- **`DebeziumTransform` or `CdcConstants` changes:** these mean re-porting our patches (step 2).
- **The minimum Java version:** this decides the worker image (step 5).
- **Breaking changes:** removed classes, and AWS SDK client changes. 1.12 moved the default AWS
  HTTP client to `apache5-client`.

Then check that every setting our connectors use still exists in the new
`kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/IcebergSinkConfig.java`.

## 2. Re-port the transform

```bash
git clone --depth 1 --branch apache-iceberg-<version> https://github.com/apache/iceberg.git /tmp/iceberg-src
T=kafka-connect/kafka-connect-transforms/src/main/java/org/apache/iceberg/connect/transforms
# What changed upstream since the version we last ported from
git -C /tmp/iceberg-src fetch --depth 1 origin tag apache-iceberg-<previous>
git -C /tmp/iceberg-src diff apache-iceberg-<previous> apache-iceberg-<version> -- $T/DebeziumTransform.java $T/CdcConstants.java
```

- **If upstream did not change `DebeziumTransform`:** only bump the versions in `build.gradle`.
- **If it did:** start again from the new upstream class and re-apply our patch. Then change the
  package to `co.factorial.iceberg.connect.transforms` and import
  `org.apache.iceberg.connect.transforms.CdcConstants`. To get our patch as a diff, compare
  `DebeziumTransform.java` in this directory with the upstream file at the version it came from.

Bump `icebergVersion`, `kafkaVersion` (the `kafka` version in the upstream
`gradle/libs.versions.toml`), `version`, and the test library versions in `build.gradle`. Then:

```bash
gradle -p factorial-transforms test jar
```

All tests must pass. They cover both patches, both code paths (with and without a schema), and
both config rejections.

## 3. Build the sink runtime

```bash
cd /tmp/iceberg-src
./gradlew -x test -x integrationTest :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip
ls kafka-connect/kafka-connect-runtime/build/distributions/
```

- The build needs Java 17 or 21 and takes a few minutes.
- A shallow clone labels the jars `<next>-SNAPSHOT`, because the version is read from git
  history. The code is still the tagged release. Clone without `--depth` for exact version
  labels.
- The default distribution has no Hive or Hadoop filesystem support, which is fine for
  `GlueCatalog` with `S3FileIO`. It cannot run `HadoopCatalog`, which fails on a missing
  `commons-configuration2`.

## 4. Assemble the plugin

```bash
unzip iceberg-kafka-connect-runtime-*.zip -d plugin/
cp factorial-transforms/build/libs/factorial-iceberg-transforms-*.jar plugin/iceberg-kafka-connect-runtime-*/lib/
```

## 5. Pick the worker image

Check the JVM before anything else:

```bash
docker run --rm --entrypoint java confluentinc/cp-kafka-connect:<tag> -version
```

| `cp-kafka-connect` | Java |
| --- | --- |
| 7.4.6 (production today) | 11 |
| 7.7.1, 7.8.0 | 17 |

Keep the Avro converter version (`AVRO_VERSION` in `build.sh`) compatible with the base.

## 6. Dry-run against production data

`dryrun/` replays real records from production through the new plugin locally, then compares
the result with production bronze row by row. It reads production only: the topic partition is
assigned, never subscribed, and offsets are never committed, so no consumer group appears on the
production cluster. The Athena queries are `SELECT`s.

Needs: Docker, `duckdb`, Python with `confluent-kafka` and `boto3`, kube access to
`aws-prod-eucentral1-glob01-blue`, and an AWS session on `Data_AWS-Production`.

```bash
cd factorial-transforms/dryrun
cp -r <plugin dir from step 4> plugin/          # dryrun/plugin/iceberg-kafka-connect-runtime-*/
python3 -m venv .venv && .venv/bin/pip install confluent-kafka boto3
bash fetch-secrets.sh                            # the sink's own Kafka + Schema Registry credentials, mode 600
docker compose up -d --build

# Copy a window: <count> records, starting <back> records before the end. Pick a window from
# a few hours back so production has long since committed it.
for t in event_store_events event_store_events_in_streams; do
  .venv/bin/python replay.py production.azure.pub.avro.rds.factorial.$t 2000 60000
done | tee replay.out

# Start one connector per topic, mapping local offset 0 onto the window's first production offset
python3 run.py $(awk '{print $1, $2}' replay.out)
```

Wait for a snapshot on both tables (`total-records` should equal the replayed count):

```bash
for t in event_store_events event_store_events_in_streams; do
  curl -s localhost:8181/v1/namespaces/data_lake_bronze/tables/$t | python3 -c \
    "import sys,json;m=json.load(sys.stdin)['metadata'];s=[x for x in m['snapshots'] if x['snapshot-id']==m.get('current-snapshot-id')];print('$t',s[0]['summary']['total-records'] if s else None)"
done
```

If a task fails with `UncheckedSQLException ... iceberg_namespace_properties`, it is the test REST
catalog racing two connectors that create the namespace at once. Restart that task. Restarting
the catalog container empties it, which triggers the race again.

Export the local tables, then compare them with production. Both sides are reduced to the same
text form (epoch microseconds, md5 of binary columns), so the two engines' formatting cannot
cause a false difference:

```bash
for t in event_store_events event_store_events_in_streams; do
  loc=$(curl -s localhost:8181/v1/namespaces/data_lake_bronze/tables/$t | python3 -c "import sys,json;print(json.load(sys.stdin)['metadata-location'])")
  duckdb -c "INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
    CREATE SECRET (TYPE s3, KEY_ID 'local', SECRET 'local', REGION 'eu-central-1', ENDPOINT 'localhost:9090', URL_STYLE 'path', USE_SSL false);
    COPY (SELECT * FROM iceberg_scan('$loc')) TO 'local_$t.parquet' (FORMAT parquet);"
done
.venv/bin/python compare.py event_store_events id:str event_type:str metadata:bin data:bin created_at:ts
.venv/bin/python compare.py event_store_events_in_streams id:str stream:str position:str event_id:str created_at:ts
```

**Pass:**

- the same row count on both sides;
- 0 rows only in local and 0 rows only in production;
- no column listed as mismatching;
- `cell_id` populated.

Also compare the Iceberg schema, partition spec, format version and table properties with
production's own metadata file (`Parameters.metadata_location` from `aws glue get-table`).

Clean up:

```bash
rm -rf secrets
docker compose down -v
```

What the dry-run does not cover: the Glue commit, IRSA, and the production consumer groups.

## 7. Build and push the image **(not yet done for the Apache sink)**

In `factorial-infrastructure/docker/connect-iceberg/`:

1. Change the base image in `Dockerfile` to a Java 17 tag.
2. Replace the Tabular zip with the plugin from step 4.
3. Set the new tag in `build.sh`, and in `cluster.yaml` under `image.application`.

Traps from earlier Tabular builds, which still apply:

- `build.sh` needs `AWS_PROFILE=Data_AWS-Production`.
- On macOS a non-interactive `docker push` is blocked by the Keychain. Use
  `docker --config <tmpdir>` with an empty `credsStore`.
- ECR pushes from inside the Docker VM hit `TLS handshake timeout`. Retry.
- Before rolling out, load the plugin inside the pushed image and check that both the sink and
  `co.factorial.iceberg.connect.transforms.DebeziumTransform` appear in
  `GET /connector-plugins?connectorsOnly=false`.

## 8. Cutover **(not yet done; needs a written plan)**

Switching from Tabular to Apache is a one-time cutover. Later Apache-to-Apache upgrades are only
steps 1 to 7 plus a connector restart.

Two things differ:

- **The class names:** `io.tabular.iceberg.connect.IcebergSinkConnector` becomes
  `org.apache.iceberg.connect.IcebergSinkConnector`, and `io.tabular...DebeziumTransform` becomes
  `co.factorial.iceberg.connect.transforms.DebeziumTransform`.
- **Where offsets are tracked:** the Apache sink uses `iceberg.connect.group-id` (default
  `connect-<connector name>`) and `iceberg.control.group-id-prefix`, not Tabular's
  `iceberg.control.group-id`. A cutover that ignores this either re-reads retained Kafka (writing
  duplicates into bronze) or skips records.

Order: events-122001, verify a few commit cycles in Glue, then pro-total.

## Current production build (Tabular fork)

Until the cutover, fixes to the running sink are built from the Tabular modules of this repo:

- Run `clean` before `distZip`. Otherwise `manifest.json` keeps a stale version while the file
  name looks right, and `confluent-hub` reads the manifest.
- `-Prelease.version` must be semver. Build `0.6.20`, then rename the zip.
- `hadoop-shaded-guava` is forced to the Maven Central `1.1.1`, because Tabular's repository
  returns 403.
- The testcontainers integration suite cannot run on Docker 29. Verify with unit tests.

## Log

| Date | Change | Found |
| --- | --- | --- |
| 2026-10-15 | Ported the transform onto Apache 1.12.0 (#3) and dry-ran it against production | Iceberg 1.12 needs Java 17 and 7.4.6 is Java 11; the commit stall on topics with no new records; the Apache runtime is not published (Confluent Hub stops at 1.9.2); MinIO images are gone from Docker Hub and quay, so the dry-run uses `adobe/s3mock`. 2,000 rows per event table matched production exactly |
