#!/usr/bin/env bash
# Copies the iceberg-sink-data Kafka and Schema Registry credentials into ./secrets (mode 600)
# for the local dry-run. Prints nothing secret. Delete ./secrets when done.
set -euo pipefail
cd "$(dirname "$0")"
umask 077
mkdir -p secrets
ctx=arn:aws:eks:eu-central-1:771567148620:cluster/aws-prod-eucentral1-glob01-blue

kubectl --context "$ctx" -n data-mesh get secret kafka-data-credentials-jaas -o json | python3 -c '
import sys, json, base64, re
d = json.load(sys.stdin)["data"]
v = base64.b64decode(next(iter(d.values()))).decode()
u = re.search(r"username=\"([^\"]+)\"", v).group(1)
p = re.search(r"password=\"([^\"]+)\"", v).group(1)
open("secrets/kafka.creds", "w").write(u + " " + p + "\n")
'

kubectl --context "$ctx" -n data-mesh get secret registry-data-userinfo -o json | python3 -c '
import sys, json, base64
d = json.load(sys.stdin)["data"]
v = base64.b64decode(next(iter(d.values()))).decode().strip()
# the connector reads it as ${file:...:auth}, so keep the same key=value shape
v = v.split("=", 1)[1] if v.startswith("auth=") else v
open("secrets/sr.properties", "w").write("auth=" + v + "\n")
'
ls -l secrets | awk 'NR>1 {print $1, $NF}'
