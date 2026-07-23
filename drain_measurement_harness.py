# Databricks notebook source
"""Databricks notebook/script for measuring Consumer-1 S3/Auto Loader drains.

Deploy this file as a Databricks notebook or Python job on the same cluster shape and
AWS region as production, set the widgets, and run LISTING_ONLY first. BOUNDED_DRAIN
uses Auto Loader availableNow with foreachBatch counting; it never writes bronze data.

By DEFAULT (empty ``run_id`` widget) every run uses FRESH schema/checkpoint directories
below a required ``_measurement`` namespace, so production tables and production Auto
Loader progress remain untouched. Because the checkpoint starts empty, that run re-reads
the ENTIRE historical backlog: it measures COLD-START, full-history drain.

To instead measure STEADY-STATE (incremental) drain — the real 24x7 loop wave — pin the
``run_id`` widget to a stable token so RUN_ROOT (hence each per-collection schema and
checkpoint path) is REUSED across runs:
  (1) first run with ``run_id=my-steady-probe`` SEATS Auto Loader offsets by draining the
      current backlog;
  (2) WAIT a representative interval for new files to arrive;
  (3) re-run with the SAME ``run_id=my-steady-probe`` — now ``drain_seconds``/files reflect
      only the INCREMENTAL new files (the steady-state wave), not the full history.
Reuse never leaves the ``_measurement/{run_id}`` namespace and still uses the count-only
foreachBatch sink, so production tables and production checkpoints stay untouched either
way. Delete the run directory and optional scratch results table when measurements are done.
"""

# VERIFY AT DEPLOY:
# - Confirm the source glob shape and per-tier maxFilesPerTrigger caps match the deployed pipeline.
# - Confirm GROUP1 still contains the deployed Consumer-1 parents.
# - Confirm decomposed children still use the ``<parent>_...`` naming convention.
# - Run on the real pipeline's cluster/runtime because driver, worker, and S3 behavior are unverified.

import json
import math
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pyspark.sql import functions as F


# COMMAND ----------

dbutils.widgets.text("catalog", "refyne_databricks_poc")
dbutils.widgets.text("schema", "refyne")
dbutils.widgets.text("database", "refyne")
dbutils.widgets.text("source_root", "s3://refyne-data-lake/kafka-test/data-lake")
dbutils.widgets.text(
    "checkpoint_root",
    "/Volumes/refyne_databricks_poc/refyne/landing/_checkpoints/_measurement",
)
dbutils.widgets.text("measurement_namespace", "drain_measurement")
dbutils.widgets.text("max_workers", "8")
dbutils.widgets.dropdown("mode", "LISTING_ONLY", ["LISTING_ONLY", "BOUNDED_DRAIN"])
dbutils.widgets.dropdown("write_results", "false", ["false", "true"])
dbutils.widgets.text("results_table", "drain_measurement_results")
# Empty (default) => a fresh RUN_ID per run (cold-start, full-history drain, unchanged
# behavior). Set to a stable token (e.g. "my-steady-probe") to PIN/REUSE that run's seated
# per-collection checkpoints on the next run and measure INCREMENTAL/steady-state drain.
dbutils.widgets.text("run_id", "")

CATALOG = dbutils.widgets.get("catalog").strip()
PROD_SCHEMA = dbutils.widgets.get("schema").strip()  # context only; never written
DATABASE = dbutils.widgets.get("database").strip()
SOURCE_ROOT = dbutils.widgets.get("source_root").rstrip("/")
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root").rstrip("/")
MEASUREMENT_NAMESPACE = dbutils.widgets.get("measurement_namespace").strip()
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))
MODE = dbutils.widgets.get("mode").upper()
WRITE_RESULTS = dbutils.widgets.get("write_results").lower() == "true"
RESULTS_TABLE = dbutils.widgets.get("results_table").strip()
# Read RAW (do NOT .strip()): only the LITERAL empty string means "use the default fresh
# UUID". Any other value — including one with leading/trailing/internal whitespace — is
# treated as provided and MUST go through _run_id_token, which rejects it (whitespace is not
# in the allowed charset). Pre-stripping here would silently accept "  token  " as "token"
# (RUN_ID no longer the EXACT supplied value) and turn whitespace-only "   " into the fresh
# path instead of raising.
RUN_ID_OVERRIDE = dbutils.widgets.get("run_id")


def _run_id_token(value, label):
    # A pinned run_id is embedded directly in RUN_ROOT, so it must be a conservative,
    # single-segment path token: no separators, no '..', no whitespace. This keeps RUN_ROOT
    # under CHECKPOINT_ROOT/MEASUREMENT_NAMESPACE (the '_measurement' SAFETY STOPs stay intact)
    # because a run_id matching this pattern cannot contain '/' to escape the namespace. The
    # explicit ".." substring reject also forbids tokens like "a..b" so the 'no ..' guarantee holds.
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value) or value in {".", ".."} or ".." in value:
        raise ValueError(
            f"{label} must match ^[A-Za-z0-9._-]+$ with no '..' (a single safe path token, no "
            f"separators, whitespace, or '..'): {value!r}")
    return value


# Empty run_id => fresh timestamp+uuid RUN_ID per run (unchanged default: cold-start, full
# history re-read). Any non-empty run_id (including whitespace-only) => that exact value is
# validated and REUSED, so RUN_ROOT (and the per-collection schema/checkpoint paths below it)
# is stable across runs and the next run with the SAME run_id drains only the incremental
# delta (steady-state wave). The raw != "" comparison ensures a whitespace-only value is
# treated as 'provided' and hits the validator (raises), never silently the fresh path.
REUSED_CHECKPOINT = RUN_ID_OVERRIDE != ""
if REUSED_CHECKPOINT:
    RUN_ID = _run_id_token(RUN_ID_OVERRIDE, "run_id")
else:
    RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]


def _identifier(value, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} must be a simple SQL identifier: {value!r}")
    return value


for _value, _label in ((CATALOG, "catalog"), (MEASUREMENT_NAMESPACE, "measurement_namespace"),
                       (RESULTS_TABLE, "results_table")):
    _identifier(_value, _label)
if MODE not in {"LISTING_ONLY", "BOUNDED_DRAIN"}:
    raise ValueError("mode must be LISTING_ONLY or BOUNDED_DRAIN")
if MAX_WORKERS < 1:
    raise ValueError("max_workers must be positive")
if "_measurement" not in CHECKPOINT_ROOT.lower():
    raise ValueError("SAFETY STOP: checkpoint_root must contain '_measurement'")
if "_measurement" not in MEASUREMENT_NAMESPACE.lower():
    raise ValueError("SAFETY STOP: measurement_namespace must contain '_measurement'")
if MEASUREMENT_NAMESPACE == PROD_SCHEMA:
    raise ValueError("SAFETY STOP: measurement_namespace must differ from production schema")

RUN_ROOT = f"{CHECKPOINT_ROOT}/{MEASUREMENT_NAMESPACE}/{RUN_ID}"
RESULTS_FQN = f"{CATALOG}.{MEASUREMENT_NAMESPACE}.{RESULTS_TABLE}"


# Consumer-1 parents and production-equivalent file caps.
GROUP1 = {
    "high": {"max_files": 3000, "topics": [
        "userwithdrawalaccessibilities", "transactions", "adminreviews", "foreclosurerequests",
        "contractors", "preapprovals", "videokyccallrequests", "disbursementpermissiondenieds",
        "experimentscreens", "users", "leads", "attendances", "underwritingauditlogs",
        "salarytopups"]},
    "medium": {"max_files": 500, "topics": [
        "videokycs", "videokycpreprocessings", "userpaymentlinks", "paymentlinks", "autokycs",
        "videokycv2", "videokycadmins", "userlocations", "departments"]},
    "low": {"max_files": 100, "topics": [
        "otps", "refundv2", "batchjobs", "principalemployers", "userauthproviders",
        "digigoldaccounts", "digigoldtransactions", "surveyresponseactions", "surveys",
        "lenderloanentityreferences", "deductions", "collectionwebhooks",
        "leadworkermatchattempts", "statements", "debitnotes", "employers", "insurancepolicies",
        "billingcycles", "salarytopupconfigs", "upiaccounts", "lenderretailodloanapplications",
        "upitransactions", "bankstatementanalyserrequests", "accountaggregatorrequests",
        "bankchangerequests", "admintransactions", "ui-elements", "subscriptions"]},
}

# ============================== ADD NEW TOPICS HERE ==============================
# Append parent collections below WITHOUT editing the GROUP1 lists above. How to add one:
#   * Use the BARE collection name only (e.g. "mynewcollection") — NO "refyne." prefix. The
#     harness keys collection identity on the bare name (see PARENTS / owner_parent below),
#     unlike the pipeline whose GROUP1 topics are "refyne.<collection>".
#   * Put it in the tier (high / medium / low) that matches the collection's write volume; the
#     tier sets its maxFilesPerTrigger cap (GROUP1[tier]["max_files"]) during BOUNDED_DRAIN.
#   * Duplicates are ignored automatically (de-duped on merge; existing order preserved).
#   * Do NOT list children — decomposed <parent>_<child> collections are auto-discovered from S3.
# Empty by default (default-inert): with all lists empty, GROUP1, PARENTS, discovery/selection,
# and every downstream measurement (listing, bounded drain, projection math, safety stops,
# results) are byte-for-byte unchanged vs today — the same Consumer-1 parents and tiers.
EXTRA_TOPICS = {
    "high": [
        # "myhighvolumecollection",
    ],
    "medium": [
        # "mymediumvolumecollection",
    ],
    "low": [
        # "mylowvolumecollection",
    ],
}

# Merge EXTRA_TOPICS into GROUP1 deterministically and SAFELY. A collection is measured under
# exactly ONE tier: PARENTS below is last-tier-wins, so the SAME collection placed under two
# tiers would silently reassign its per-tier maxFilesPerTrigger cap (GROUP1[tier]["max_files"])
# and mismeasure its drain. We therefore build ONE GLOBAL collection->tier map spanning BOTH the
# existing GROUP1 declarations AND the EXTRA_TOPICS additions, BEFORE mutating anything.
def _canon_topic(topic):
    # Canonical collection identity of a topic in the harness: the bare, trimmed collection name.
    # The harness has NO "refyne."-style prefix (topics are already bare, e.g. "transactions"),
    # so canonical identity is just the trimmed name — consistent with how PARENTS / owner_parent
    # key on the raw bare collection string below. No dash/underscore folding: "ui-elements" and
    # "ui_elements" stay distinct here, exactly as PARENTS/owner_parent already treat them.
    return topic.strip()


# Pass 1 — VALIDATE ONLY (no mutation): fold existing declarations then the extras into one
# collection->tier map. A collection assigned to two DIFFERENT tiers FAILS LOUD (names the
# collection and both tiers); the SAME collection repeated under the SAME tier is fine (idempotent).
_topic_tier = {}
for _tier in ("high", "medium", "low"):
    for _tp in list(GROUP1[_tier]["topics"]) + list(EXTRA_TOPICS[_tier]):
        _coll = _canon_topic(_tp)
        _prior = _topic_tier.get(_coll)
        if _prior is not None and _prior != _tier:
            raise ValueError(
                f"collection {_coll!r} (from topic {_tp!r}) is assigned to two tiers "
                f"({_prior!r} and {_tier!r}); a collection is measured under exactly ONE tier "
                f"(its maxFilesPerTrigger cap) — remove the duplicate from GROUP1/EXTRA_TOPICS")
        _topic_tier[_coll] = _tier

# Pass 2 — MUTATE: append each tier's extras to that tier's topics list, de-duplicating on
# CANONICAL COLLECTION IDENTITY (via _canon_topic, consistent with Pass 1) — NOT the raw string.
# Seeding the seen-set from the pre-existing GROUP1 topics is also canonicalized, so an
# EXTRA_TOPICS entry that canonically duplicates an already-declared collection in the same tier
# is a no-op (not re-added). We append the CANONICAL bare name (not the raw string) because the
# harness feeds GROUP1 topics straight into PARENTS/owner_parent/source_glob with no downstream
# canonicalization — so the stored form must already be the clean bare identity. Order preserved
# (existing topics keep their position; a new collection is appended at most once).
for _tier, _extras in EXTRA_TOPICS.items():
    _topics = GROUP1[_tier]["topics"]
    _seen = {_canon_topic(t) for t in _topics}
    for _tp in _extras:
        _coll = _canon_topic(_tp)
        if _coll not in _seen:
            _topics.append(_coll)
            _seen.add(_coll)

PARENTS = {name: tier for tier, cfg in GROUP1.items() for name in cfg["topics"]}


def owner_parent(name):
    matches = [p for p in PARENTS if name == p or name.startswith(p + "_")]
    return max(matches, key=len) if matches else None


def _ls(path):
    """List a path; callers must explicitly handle access, throttling, and missing-path errors."""
    return dbutils.fs.ls(path)


def _dirs(path):
    return [x for x in _ls(path) if x.name.endswith("/")]


def discover_collections():
    """Walk year/month/day/database and return names, reusable paths, and metrics."""
    started = time.perf_counter()
    base = f"{SOURCE_ROOT}/source=mongo"
    years = _dirs(base)
    months = [m for y in years for m in _dirs(y.path)]
    days = [d for m in months for d in _dirs(m.path)]
    db_paths = [f"{d.path}database={DATABASE}" for d in days]
    names = set()
    collection_paths = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for entries in pool.map(_ls, db_paths):
            for entry in entries:
                if entry.name.startswith("collection=") and entry.name.endswith("/"):
                    name = entry.name.rstrip("/")[len("collection="):]
                    names.add(name)
                    collection_paths.setdefault(name, []).append(entry.path)
    return names, {"years": years, "months": months, "days": days,
                   "collection_paths": collection_paths}, {
        "discovery_seconds": time.perf_counter() - started,
        "year_dirs": len(years), "month_dirs": len(months), "day_dirs": len(days),
        "partition_tree_depth": 3 if days else (2 if months else (1 if years else 0)),
    }


def source_glob(collection, fmt):
    # Hadoop brace glob includes ordinary and gzip-compressed files for the chosen format.
    extension_glob = f"{{{fmt},{fmt}.gz}}"
    return (f"{SOURCE_ROOT}/source=mongo/year=*/month=*/day=*/database={DATABASE}/"
            f"collection={collection}/*.{extension_glob}")


def _matches_format(filename, fmt):
    name = filename.lower()
    return name.endswith(f".{fmt}") or name.endswith(f".{fmt}.gz")


def collection_listing(collection, partition_tree):
    """Explicitly walk the partition tree for one collection and count visible files."""
    started = time.perf_counter()
    years = partition_tree["years"]
    months = partition_tree["months"]
    days = partition_tree["days"]
    collection_paths = partition_tree["collection_paths"].get(collection, [])
    files = []
    errors = []
    # The outer collection pool is the only listing pool: total LIST concurrency <= MAX_WORKERS.
    for path in collection_paths:
        try:
            entries = _ls(path)
            files.extend(x for x in entries if not x.name.endswith("/"))
        except Exception as exc:
            errors.append(f"{path}: {str(exc)[:300]}")
    fmt = ("parquet" if any(_matches_format(x.name, "parquet") for x in files)
           else ("csv" if any(_matches_format(x.name, "csv") for x in files) else None))
    matching_files = [x for x in files if fmt and _matches_format(x.name, fmt)]
    listing_ok = not errors
    return {
        "collection": collection,
        "listing_status": "OK" if listing_ok else "ERROR",
        "listing_error": None if listing_ok else " | ".join(errors)[:2000],
        "listing_seconds": time.perf_counter() - started,
        "partition_tree_depth": 3 if days else (2 if months else (1 if years else 0)),
        "year_dirs_walked": len(years), "month_dirs_walked": len(months),
        "day_dirs_walked": len(days), "collection_dirs_checked": len(collection_paths),
        # Partial results are deliberately unknown: never report throttling as a smaller backlog.
        "file_count": len(matching_files) if listing_ok else None,
        "total_bytes": sum(x.size for x in matching_files) if listing_ok else None,
        "format": fmt if listing_ok else None,
    }


startup_walk_started = time.perf_counter()
discovered, partition_tree, discovery = discover_collections()
selected_names = sorted(n for n in discovered if owner_parent(n))
if not selected_names:
    selected_names = sorted(PARENTS)
    print("WARNING: discovery found no Consumer-1 paths; reporting parents with zero/visible listings")

print(json.dumps({"run_id": RUN_ID, "mode": MODE, "run_root": RUN_ROOT,
                  "reused_checkpoint": REUSED_CHECKPOINT,
                  "parents_configured": len(PARENTS), "collections_discovered": len(discovered),
                  **discovery}, indent=2))

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    listing_rows = list(pool.map(lambda name: collection_listing(name, partition_tree), selected_names))
startup_walk_seconds = time.perf_counter() - startup_walk_started


# COMMAND ----------

def bounded_drain(row):
    """Drain into a no-op foreachBatch sink using a unique, throwaway checkpoint."""
    if row["listing_status"] == "ERROR":
        return {**row, "status": "ERROR", "micro_batches": None, "rows_seen": None,
                "query_start_seconds": None, "query_stop_seconds": None,
                "drain_seconds": None, "total_drain_wall_seconds": None,
                "batch_details_json": None, "error": row["listing_error"]}
    if not row["format"] or row["file_count"] == 0:
        return {**row, "status": "NO_FILES", "micro_batches": 0, "rows_seen": 0,
                "query_start_seconds": 0.0, "query_stop_seconds": 0.0,
                "drain_seconds": 0.0, "total_drain_wall_seconds": 0.0,
                "batch_details_json": "[]", "error": None}
    collection = row["collection"]
    safe_name = re.sub(r"[^A-Za-z0-9_]", "_", collection)
    tier = PARENTS[owner_parent(collection)]
    query = None
    result = None
    try:
        build_started = time.perf_counter()
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", row["format"])
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaLocation", f"{RUN_ROOT}/schemas/{safe_name}")
                  .option("cloudFiles.schemaEvolutionMode", "none")
                  .option("cloudFiles.maxFilesPerTrigger", GROUP1[tier]["max_files"])
                  .option("rescuedDataColumn", "_rescued_data"))
        if row["format"] == "csv":
            reader = reader.option("header", "true")
        df = reader.load(source_glob(collection, row["format"]))
        df.sparkSession.conf.set("spark.sql.streaming.numRecentProgressUpdates", "1000000")
        query = (df.writeStream.queryName(f"measure_{safe_name}_{RUN_ID}")
                 .format("noop")
                 .option("checkpointLocation", f"{RUN_ROOT}/checkpoints/{safe_name}")
                 .trigger(availableNow=True).start())
        query_start_seconds = time.perf_counter() - build_started
        drain_started = time.perf_counter()
        query.awaitTermination()
        drain_seconds = time.perf_counter() - drain_started
        # Collect per-batch stats from Spark progress tracking (Spark Connect compatible;
        # no Python closure is passed to Spark). recentProgress is driver-side only.
        progress_list = list(query.recentProgress)
        batches = sorted(
            [{"batch_id": int(p.get("batchId", i)),
              "rows": int(p.get("numInputRows", 0)),
              "seconds": p.get("durationMs", {}).get("triggerExecution", 0) / 1000.0}
             for i, p in enumerate(progress_list)],
            key=lambda x: x["batch_id"])
        result = {**row, "status": "OK", "micro_batches": len(batches),
                "rows_seen": sum(x["rows"] for x in batches),
                "query_start_seconds": query_start_seconds, "query_stop_seconds": None,
                "drain_seconds": drain_seconds,
                "total_drain_wall_seconds": None,
                "batch_details_json": json.dumps(batches),
                "error": None}
    except Exception as exc:
        # Salvage whatever batch progress was recorded before the failure.
        batches = []
        if query is not None:
            try:
                progress_list = list(query.recentProgress)
                batches = sorted(
                    [{"batch_id": int(p.get("batchId", i)),
                      "rows": int(p.get("numInputRows", 0)),
                      "seconds": p.get("durationMs", {}).get("triggerExecution", 0) / 1000.0}
                     for i, p in enumerate(progress_list)],
                    key=lambda x: x["batch_id"])
            except Exception:
                pass
        result = {**row, "status": "ERROR", "micro_batches": len(batches),
                "rows_seen": sum(x["rows"] for x in batches), "query_start_seconds": None,
                "query_stop_seconds": None, "drain_seconds": None,
                "total_drain_wall_seconds": None,
                "batch_details_json": json.dumps(batches), "error": str(exc)[:1000]}
    finally:
        stop_started = time.perf_counter()
        if query is not None:
            try:
                query.stop()
            except Exception as stop_exc:
                if result is not None:
                    result["error"] = ((result.get("error") or "") +
                                       f"; query.stop failed: {str(stop_exc)[:500]}").lstrip("; ")
                    result["status"] = "ERROR"
        stop_seconds = time.perf_counter() - stop_started
        if result is not None:
            result["query_stop_seconds"] = stop_seconds
            if result["query_start_seconds"] is not None and result["drain_seconds"] is not None:
                result["total_drain_wall_seconds"] = (
                    result["query_start_seconds"] + result["drain_seconds"] + stop_seconds)
    return result


overall_started = time.perf_counter()
if MODE == "BOUNDED_DRAIN":
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(bounded_drain, row): row["collection"] for row in listing_rows}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"[{result['status']}] {result['collection']}: "
                  f"files={result['file_count']}, batches={result['micro_batches']}, "
                  f"drain_s={result['drain_seconds']}")
else:
    results = [{**row, "status": "LISTED" if row["listing_status"] == "OK" else "ERROR",
                "micro_batches": None, "rows_seen": None,
                "query_start_seconds": None, "query_stop_seconds": None,
                "drain_seconds": None, "total_drain_wall_seconds": None,
                "batch_details_json": None, "error": row["listing_error"]}
               for row in listing_rows]
overall_wall_seconds = time.perf_counter() - overall_started


def projected_seconds(durations, workers):
    """LPT model: longest measured drain is assigned to the next free executor slot."""
    slots = [0.0] * workers
    for duration in sorted(durations, reverse=True):
        i = slots.index(min(slots))
        slots[i] += duration
    return max(slots, default=0.0)


durations = [r["drain_seconds"] + r["query_start_seconds"] + r["query_stop_seconds"]
             for r in results if r["status"] == "OK"]
projections = [{"max_workers": w,
                "simple_ceil_collection_rounds": math.ceil(len(durations) / w) if durations else 0,
                "lpt_projected_full_drain_seconds": projected_seconds(durations, w)}
               for w in (4, 6, 8, 12, 16)]
parents_present = sum(1 for n in selected_names if n in PARENTS)
summary = {
    "run_id": RUN_ID, "mode": MODE, "reused_checkpoint": REUSED_CHECKPOINT,
    "configured_max_workers": MAX_WORKERS,
    "parents_present": parents_present, "children_discovered": len(selected_names) - parents_present,
    "collections_measured": len(results),
    "errored_collections": sum(1 for r in results if r["status"] == "ERROR"),
    "visible_files": sum(r["file_count"] for r in results if r["file_count"] is not None),
    "visible_bytes": sum(r["total_bytes"] for r in results if r["total_bytes"] is not None),
    "discovery_seconds": discovery["discovery_seconds"],
    "discovery_and_format_detection_seconds": startup_walk_seconds,
    "full_set_wall_seconds": overall_wall_seconds, "projections": projections,
}
print("\n=== DRAIN MEASUREMENT SUMMARY ===")
print(json.dumps(summary, indent=2))
print("\n=== PER COLLECTION ===")
for row in sorted(results, key=lambda x: x["collection"]):
    print(json.dumps(row, sort_keys=True))


# COMMAND ----------

if WRITE_RESULTS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{MEASUREMENT_NAMESPACE}")
    output = [{**r, "run_id": RUN_ID, "mode": MODE,
               "measured_at_utc": datetime.now(timezone.utc).isoformat(),
               "full_set_wall_seconds": overall_wall_seconds,
               "discovery_seconds": discovery["discovery_seconds"]}
              for r in results]
    # JSON inference represents all-null LISTING_ONLY fields safely (createDataFrame cannot infer them).
    # Build the single-column string DataFrame WITHOUT the RDD API (no spark.sparkContext /
    # parallelize) so this stays Spark-Connect-safe (shared access mode, serverless, DBR Connect);
    # spark.read.json still does the all-null-safe JSON string inference. Do NOT reintroduce RDD APIs.
    json_rows = [(json.dumps(x),) for x in output]
    output_df = spark.read.json(
        spark.createDataFrame(json_rows, "value string").select("value"))
    (output_df.withColumn("measured_at_utc", F.to_timestamp("measured_at_utc"))
     .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(RESULTS_FQN))
    print(f"Appended {len(output)} rows to scratch table {RESULTS_FQN}")

print(f"Measurement artifacts (safe to delete): {RUN_ROOT}")
