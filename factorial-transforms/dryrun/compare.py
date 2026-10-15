"""Compare the dry-run tables with prod bronze, row by row, over the replayed offset range.

Both sides are reduced to the same text form (timestamps as epoch micros, binary as md5) so
that two engines' formatting cannot cause a false mismatch.

usage: compare.py <table> <col:kind> ...   kind is one of str, ts, bin
"""
import subprocess
import sys
import time

import boto3

table, cols = sys.argv[1], [c.split(":") for c in sys.argv[2:]]
CDC = [("_cdc.op", "str"), ("_cdc.ts", "ts"), ("_cdc.offset", "str"), ("_cdc.source", "str"),
       ("_cdc.target", "str"), ("_cdc.key.id", "str"), ("_cdc.cell_id", "str"), ("_event_ts", "ts")]
allcols = [(c, k) for c, k in cols] + CDC


def expr(col, kind, engine):
    if kind == "ts":
        return f"epoch_us({col})" if engine == "duck" else f"cast(round(to_unixtime({col}) * 1e6) as bigint)"
    if kind == "bin":
        return f"md5({col})" if engine == "duck" else f"lower(to_hex(md5({col})))"
    return f"cast({col} as varchar)"


def select(engine):
    return ", ".join(f'cast({expr(c, k, engine)} as varchar) as "{c.replace(".", "__")}"' for c, k in allcols)


def duck(sql):
    return subprocess.run(["duckdb", "-csv", "-noheader", "-c", sql], check=True, capture_output=True, text=True).stdout.strip()


lo, hi, ts_lo, ts_hi = duck(
    f"select min(_cdc.offset), max(_cdc.offset), epoch(min(_event_ts))::bigint, epoch(max(_event_ts))::bigint "
    f"from 'local_{table}.parquet'").split(",")

athena = boto3.Session(profile_name="Data_AWS-Production", region_name="eu-central-1").client("athena")
sql = (f"select {select('athena')} from data_lake_bronze.{table} "
       f"where _cdc.offset between {lo} and {hi} "
       f"and _event_ts between from_unixtime({ts_lo} - 3600) and from_unixtime({ts_hi} + 3600)")
qid = athena.start_query_execution(QueryString=sql, WorkGroup="primary")["QueryExecutionId"]
while (state := athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"])["State"] in ("QUEUED", "RUNNING"):
    time.sleep(2)
if state["State"] != "SUCCEEDED":
    raise SystemExit(state)
out = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
boto3.Session(profile_name="Data_AWS-Production").client("s3").download_file(
    out.split("/")[2], "/".join(out.split("/")[3:]), f"prod_{table}.csv")

names = [c.replace(".", "__") for c, _ in allcols]
local = f"(select {select('duck')} from 'local_{table}.parquet')"
prod = f"read_csv('prod_{table}.csv', header=true, all_varchar=true)"
print(f"== {table}  offsets {lo}..{hi}")
print("rows local,prod:", duck(f"select (select count(*) from {local}), (select count(*) from {prod})"))
print("distinct offsets in prod:", duck(f"select count(distinct _cdc__offset) from {prod}"))
print("rows only local:", duck(f"select count(*) from (select * from {local} except all select * from {prod})"))
print("rows only prod :", duck(f"select count(*) from (select * from {prod} except all select * from {local})"))
for n in names:
    bad = duck(f"select count(*) from {local} l join {prod} p using (_cdc__offset) "
               f"where l.\"{n}\" is distinct from p.\"{n}\"")
    if bad != "0":
        print(f"  column {n}: {bad} mismatches, e.g.",
              duck(f"select l.\"{n}\", p.\"{n}\" from {local} l join {prod} p using (_cdc__offset) "
                   f"where l.\"{n}\" is distinct from p.\"{n}\" limit 2").replace("\n", " | "))
print("cell_id values:", duck(f"select string_agg(v || '=' || n, ', ') from (select _cdc__cell_id v, count(*) n from {local} group by 1)"))
