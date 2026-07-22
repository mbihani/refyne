# Databricks notebook source
# MAGIC %md
# MAGIC # Consumer-1 bronze ingestion — all collections (multithreaded)
# MAGIC
# MAGIC One job that ingests **every collection in consumer group1** (high + medium + low)
# MAGIC from the S3 landing zone into `poc_bronze_<collection>` via Auto Loader / Spark
# MAGIC Structured Streaming. Replaces the standalone per-collection notebooks.
# MAGIC
# MAGIC **Design for the 6-core `cluster_poc_classical`:**
# MAGIC - **Child tables included.** The topics are parent collections; each parent's
# MAGIC   decomposed children land in S3 as `collection=<parent>_<child>`. The job discovers
# MAGIC   them from S3 and ingests them too (so stream count > number of topics).
# MAGIC - **Format is auto-detected per collection** (parquet vs csv) by probing S3 — no
# MAGIC   hardcoded format map. Collections with no files are skipped.
# MAGIC - **Bounded multithreading:** `max_concurrent_streams` streams run at once. In
# MAGIC   `availableNow` mode each drains its files then stops, freeing the slot — so 51
# MAGIC   collections flow through a small cluster in waves instead of thrashing.
# MAGIC - **Mirrors skynet's consumer design.** Per-tier `max_batch_size` →
# MAGIC   `cloudFiles.maxFilesPerTrigger` and `batch_duration_seconds` → the micro-batch
# MAGIC   `trigger(processingTime=...)`, exactly like the Kafka consumer's batching. In
# MAGIC   `realtime` each collection gets its own **supervisor thread** (skynet's
# MAGIC   one-thread-per-topic) that restarts its stream on failure (skynet's re-session loop).
# MAGIC
# MAGIC **Two ways to run continuously across all collections:**
# MAGIC - `trigger_mode=availableNow` as a **scheduled Job** (e.g. every 2–5 min) → each run
# MAGIC   drains once on a fresh/job cluster, no cluster pinned 24/7.
# MAGIC - `trigger_mode=loop` on a **persistent / warm classic cluster** → the same bounded
# MAGIC   `availableNow` wave-drain runs in an always-on driver loop (drain → sleep
# MAGIC   `loop_interval_seconds` → repeat). It is a **single process**, so there is exactly
# MAGIC   **one writer per checkpoint** and no concurrent-run checkpoint contention. This is
# MAGIC   the intended 24×7 mode for `cluster_poc_classical`.
# MAGIC
# MAGIC `realtime` (continuous) needs a bigger cluster for this many streams.
# MAGIC
# MAGIC **Before running:** stop the old single-collection transactions job (this one also
# MAGIC ingests `transactions`, and two writers on one checkpoint clash). Likewise, never run
# MAGIC `loop` and a scheduled `availableNow` against the same checkpoints at the same time.

# COMMAND ----------

dbutils.widgets.text("catalog", "refyne_databricks_poc")
dbutils.widgets.text("schema", "refyne")
dbutils.widgets.text("database", "refyne")  # the mongo db in the S3 path (database=...)
dbutils.widgets.text("source_root", "s3://refyne-data-lake/kafka-test/data-lake")
dbutils.widgets.text("checkpoint_root", "/Volumes/refyne_databricks_poc/refyne/landing/_checkpoints")
dbutils.widgets.text("dest_prefix", "poc_")
# trigger_mode:
#   availableNow — one bounded wave-drain then stop (once / scheduled Job on a schedule).
#   loop         — same bounded wave-drain, but repeated forever with a sleep between waves;
#                  designed to run 24×7 on a PERSISTENT/WARM classic cluster (see Run cell).
#   realtime     — one continuous supervisor thread per collection.
dbutils.widgets.dropdown("trigger_mode", "availableNow", ["availableNow", "loop", "realtime"])
dbutils.widgets.dropdown("priority", "all", ["all", "high", "medium", "low"])
# Bounded per our sizing analysis for the 6-core cluster — keep in the 6–8 range.
dbutils.widgets.text("max_concurrent_streams", "8")
# Warm-cluster drain loop: seconds to sleep between availableNow waves (loop mode only).
dbutils.widgets.text("loop_interval_seconds", "120")
# Parallel sharding — run N jobs in parallel, each owning a DISJOINT subset of parents.
#   shard_count = how many parallel jobs (N); shard_index = which shard THIS run is (0..N-1).
# Defaults (shard_count=1, shard_index=0) mean "one run owns everything" -> no behavior change.
dbutils.widgets.text("shard_count", "1")
dbutils.widgets.text("shard_index", "0")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATABASE = dbutils.widgets.get("database")
SOURCE_ROOT = dbutils.widgets.get("source_root")
CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root")
DEST_PREFIX = dbutils.widgets.get("dest_prefix")
TRIGGER_MODE = dbutils.widgets.get("trigger_mode")
PRIORITY = dbutils.widgets.get("priority")
MAX_CONCURRENT_CAP = 32  # sizing guardrail for the small classic cluster


def _require_positive_int(name, raw, cap=None):
    # Validate widget input and FAIL LOUDLY on bad values rather than silently proceeding.
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"widget {name!r}={raw!r} must be an integer")
    if value <= 0:
        raise ValueError(f"widget {name!r} must be a positive integer (got {value})")
    if cap is not None and value > cap:
        print(f"WARNING: {name}={value} exceeds cap {cap}; clamping to {cap}")
        value = cap
    return value


def _require_nonneg_int(name, raw):
    # Same fail-loud style as _require_positive_int, but 0 is allowed (shard_index is 0-based).
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"widget {name!r}={raw!r} must be an integer")
    if value < 0:
        raise ValueError(f"widget {name!r} must be a non-negative integer (got {value})")
    return value


# Clamp concurrency to a sane positive range (no zero/negative; default 8, capped) and
# require a positive loop interval — invalid input aborts the run instead of proceeding.
MAX_CONCURRENT = _require_positive_int(
    "max_concurrent_streams", dbutils.widgets.get("max_concurrent_streams"), cap=MAX_CONCURRENT_CAP)
LOOP_INTERVAL_SECONDS = _require_positive_int(
    "loop_interval_seconds", dbutils.widgets.get("loop_interval_seconds"))

# Parallel sharding knobs: shard_count >= 1 and 0 <= shard_index < shard_count. Defaults
# (1, 0) keep every parent in this run. Invalid input aborts the run instead of proceeding.
SHARD_COUNT = _require_positive_int("shard_count", dbutils.widgets.get("shard_count"))
SHARD_INDEX = _require_nonneg_int("shard_index", dbutils.widgets.get("shard_index"))
if not (0 <= SHARD_INDEX < SHARD_COUNT):
    raise ValueError(f"shard_index={SHARD_INDEX} must satisfy 0 <= shard_index < shard_count "
                     f"(shard_count={SHARD_COUNT})")

# COMMAND ----------

# Consumer group1 — topics per priority tier. max_batch_size drives Auto Loader's
# maxFilesPerTrigger for that tier (a rough analog of the consumer's batch size).
GROUP1 = {
    "high": {
        "max_batch_size": 3000,
        "batch_duration_seconds": 90,
        "topics": [
            "refyne.userwithdrawalaccessibilities", "refyne.transactions", "refyne.adminreviews",
            "refyne.foreclosurerequests", "refyne.contractors", "refyne.preapprovals",
            "refyne.videokyccallrequests", "refyne.disbursementpermissiondenieds",
            "refyne.experimentscreens", "refyne.users", "refyne.leads", "refyne.attendances",
            "refyne.underwritingauditlogs", "refyne.salarytopups",
        ],
    },
    "medium": {
        "max_batch_size": 500,
        "batch_duration_seconds": 90,
        "topics": [
            "refyne.videokycs", "refyne.videokycpreprocessings", "refyne.userpaymentlinks",
            "refyne.paymentlinks", "refyne.autokycs", "refyne.videokycv2", "refyne.videokycadmins",
            "refyne.userlocations", "refyne.departments",
        ],
    },
    "low": {
        "max_batch_size": 100,
        "batch_duration_seconds": 90,
        "topics": [
            "refyne.otps", "refyne.refundv2", "refyne.batchjobs", "refyne.principalemployers",
            "refyne.userauthproviders", "refyne.digigoldaccounts", "refyne.digigoldtransactions",
            "refyne.surveyresponseactions", "refyne.surveys", "refyne.lenderloanentityreferences",
            "refyne.deductions", "refyne.collectionwebhooks", "refyne.leadworkermatchattempts",
            "refyne.statements", "refyne.debitnotes", "refyne.employers", "refyne.insurancepolicies",
            "refyne.billingcycles", "refyne.salarytopupconfigs", "refyne.upiaccounts",
            "refyne.lenderretailodloanapplications", "refyne.upitransactions",
            "refyne.bankstatementanalyserrequests", "refyne.accountaggregatorrequests",
            "refyne.bankchangerequests", "refyne.admintransactions", "refyne.ui-elements",
            "refyne.subscriptions",
        ],
    },
}

# ============================== ADD NEW TOPICS HERE ==============================
# Append topics below WITHOUT editing the GROUP1 lists above. How to add one:
#   * Format is "refyne.<collection>" — the PARENT collection name only.
#   * Put it in the tier (high / medium / low) that matches the collection's write volume.
#   * Duplicates are ignored automatically (de-duped on merge; existing order preserved).
#   * Do NOT list children — decomposed <parent>_<child> collections are auto-discovered from S3.
# Empty by default (default-inert): with all lists empty there is no topic/table/checkpoint
# behavior change vs today — existing tables and checkpoints resume unchanged (ingestion-state
# compatible). New widgets + shard-status logging change notebook OUTPUT only, not ingestion.
EXTRA_TOPICS = {
    "high": [
        # "refyne.myhighvolumecollection",
    ],
    "medium": [
        # "refyne.mymediumvolumecollection",
    ],
    "low": [
        # "refyne.mylowvolumecollection",
    ],
}

# Merge EXTRA_TOPICS into GROUP1 deterministically and SAFELY. A collection maps to exactly ONE
# bronze table + ONE checkpoint, so it must live in exactly ONE tier: the same collection placed
# under two tiers would silently reassign its batch params (PARENTS below is last-tier-wins) and
# risk two writers on one checkpoint. We therefore build ONE GLOBAL collection->tier map spanning
# BOTH the existing GROUP1 declarations AND the EXTRA_TOPICS additions, BEFORE mutating anything.
def _canon_topic(topic):
    # Canonical collection identity of a topic, mirroring collection_of() / PARENTS (defined below):
    # "refyne.transactions" -> "transactions". Inlined here because this runs earlier in the cell.
    return topic.split(".", 1)[1] if "." in topic else topic


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
                f"topic {_tp!r} (collection {_coll!r}) is assigned to two tiers "
                f"({_prior!r} and {_tier!r}); a collection maps to one bronze table + one "
                f"checkpoint, so it must belong to exactly ONE tier — remove the duplicate "
                f"from GROUP1/EXTRA_TOPICS")
        _topic_tier[_coll] = _tier

# Pass 2 — MUTATE: append each tier's extras to that tier's topics list, de-duplicating on
# CANONICAL COLLECTION IDENTITY (via _canon_topic, consistent with Pass 1) — NOT the raw topic
# string. Two raw forms that canonicalize to the same collection (e.g. "refyne.foo" and "foo")
# resolve to ONE bronze table + ONE checkpoint, so only the FIRST is appended; the rest are no-ops.
# Seeding from the pre-existing GROUP1 topics is also canonicalized, so an EXTRA_TOPICS entry that
# canonically duplicates an already-declared collection in the same tier is a no-op (not re-added).
# Order preserved (existing topics keep their position; a new collection is appended at most once).
# Downstream code (PARENTS, TIER_PARAMS, discovery, ownership) picks the additions up unchanged.
for _tier, _extras in EXTRA_TOPICS.items():
    _topics = GROUP1[_tier]["topics"]
    _seen = {_canon_topic(t) for t in _topics}
    for _tp in _extras:
        _coll = _canon_topic(_tp)
        if _coll not in _seen:
            _topics.append(_tp)
            _seen.add(_coll)

TIERS = ["high", "medium", "low"] if PRIORITY == "all" else [PRIORITY]


def collection_of(topic):
    # "refyne.transactions" -> "transactions"
    return topic.split(".", 1)[1] if "." in topic else topic


def table_name(collection):
    # Delta table names can't contain '-' / '.'; the S3 path still uses the raw collection.
    # TODO (out of scope): harden checkpoint/table-name collisions — two distinct raw
    # collections (e.g. "ui-elements" and "ui_elements") normalize to the same table AND
    # the same checkpoint path here, which would silently cross two sources onto one writer.
    return collection.replace("-", "_").replace(".", "_")


# The topics are PARENT collections. Each parent's decomposed child tables land in S3 as
# their own prefixes: collection=<parent>_<child> (e.g. transactions_statuslogs). We map
# every selected parent to its tier, then discover children from S3 (next cell) — so we
# don't have to hardcode child lists for every collection.
PARENTS = {collection_of(tp): tier for tier in TIERS for tp in GROUP1[tier]["topics"]}


def owner_parent(name):
    # A collection belongs to consumer1 if it equals a parent or is a <parent>_ child.
    # Pick the longest matching parent (handles nested names like a_b_c under a_b).
    best = None
    for p in PARENTS:
        if name == p or name.startswith(p + "_"):
            if best is None or len(p) > len(best):
                best = p
    return best


# skynet's per-tier batching, mapped onto Structured Streaming (mirrors the Kafka consumer):
#   max_batch_size         -> cloudFiles.maxFilesPerTrigger  (cap files per micro-batch)
#   batch_duration_seconds -> trigger(processingTime=...)    (micro-batch cadence, realtime)
TIER_PARAMS = {
    tier: {"max_files": cfg["max_batch_size"], "interval": cfg["batch_duration_seconds"]}
    for tier, cfg in GROUP1.items()
}


print(f"priority={PRIORITY}  trigger={TRIGGER_MODE}  parents={len(PARENTS)}  "
      f"max_concurrent={MAX_CONCURRENT}")

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F


def bronze_table(name):
    return f"{CATALOG}.{SCHEMA}.{DEST_PREFIX}bronze_{name}"


def source_glob(collection, fmt):
    # Trailing /*.<fmt> is load-bearing: it stops Auto Loader from prefix-matching a
    # sibling directory (collection=transactions also matching transactions_statuslogs).
    return (f"{SOURCE_ROOT}/source=mongo/year=*/month=*/day=*/"
            f"database={DATABASE}/collection={collection}/*.{fmt}")


def detect_format(collection):
    # Probe S3 for the file type (no hardcoded map). binaryFile is JVM-free (works on
    # serverless/shared/classic) and only lists metadata. None => no files, skip.
    # TODO (out of scope): mixed-format collections — a prefix holding BOTH parquet and csv
    # returns only the first hit here, so the other format's files are never ingested.
    for fmt in ("parquet", "csv"):
        try:
            if spark.read.format("binaryFile").load(source_glob(collection, fmt)).limit(1).count() > 0:
                return fmt
        except Exception:
            pass
    return None


def _ls(path):
    try:
        return dbutils.fs.ls(path)
    except Exception:
        return []


def _subdirs(path):
    return [fi.path for fi in _ls(path) if fi.name.endswith("/")]


def discover_collections():
    # Walk the landing layout to the database level and collect every collection= dir name
    # (parents AND decomposed children), unioned across all date partitions.
    base = f"{SOURCE_ROOT}/source=mongo"
    day_dirs = [f"{d}database={DATABASE}"
                for y in _subdirs(base) for m in _subdirs(y) for d in _subdirs(m)]

    def names_in(dbpath):
        return [fi.name.rstrip("/")[len("collection="):]
                for fi in _ls(dbpath) if fi.name.startswith("collection=")]

    names = set()
    if day_dirs:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as ex:
            for lst in ex.map(names_in, day_dirs):
                names.update(lst)
    return names


# Discover which collections actually exist in S3, then keep those owned by a consumer1
# parent (the parent itself or any of its <parent>_ children). Falls back to parents-only
# if discovery finds nothing (e.g. no S3 access).
_discovered = discover_collections()
if _discovered:
    _keep = sorted(n for n in _discovered if owner_parent(n))
    SELECTED = [(n, table_name(n), PARENTS[owner_parent(n)]) for n in _keep]
    _n_children = sum(1 for n in _keep if n not in PARENTS)
    print(f"discovered {len(_discovered)} collections in S3; ingesting {len(SELECTED)} for "
          f"consumer1 ({len(SELECTED) - _n_children} parents + {_n_children} children)")
else:
    SELECTED = [(p, table_name(p), PARENTS[p]) for p in PARENTS]
    print(f"WARNING: S3 discovery returned nothing — falling back to {len(SELECTED)} parents "
          f"only (children NOT ingested). Check S3 access / source_root.")

# --- Parallel shard scoping (default-inert) --------------------------------------------------
# Partition the parents this run just resolved round-robin across SHARD_COUNT jobs, keeping only
# this shard's parents. Sharding is at the PARENT level, so every parent's decomposed children
# ride along with their parent -> shards own DISJOINT collections -> disjoint bronze tables AND
# checkpoints -> no two jobs ever share a checkpoint/writer. With shard_count=1 (default) the
# formula keeps ALL parents, so SELECTED is untouched — no parent/table/checkpoint change vs
# today (the shard-status log line is the only added output).
#
# PARALLEL RUN RECIPE (mirrors the 3-pipeline EKS model; K=3, ~40 topics each):
#   * Deploy K Databricks Jobs, all pointing at THIS notebook, all with shard_count=K.
#   * Give each job a distinct shard_index in 0..K-1.
#   * Run each job on its OWN classic cluster, each with the Job setting max_concurrent_runs=1.
# Scales past K=3 the same way — bump shard_count and add jobs.
_owned_parents = sorted({owner_parent(coll) for coll, _tbl, _tier in SELECTED})
if SHARD_COUNT > 1:
    _shard_parents = {p for i, p in enumerate(_owned_parents) if i % SHARD_COUNT == SHARD_INDEX}
    SELECTED = [item for item in SELECTED if owner_parent(item[0]) in _shard_parents]
else:
    _shard_parents = set(_owned_parents)
print(f"shard {SHARD_INDEX}/{SHARD_COUNT}: this run owns {len(_shard_parents)} of "
      f"{len(_owned_parents)} parent(s) after sharding "
      f"({len(SELECTED)} collection(s) incl. children)")

# Detect all formats in parallel (one-time, at job start). Children inherit their parent's tier.
def _resolve(item):
    collection, name, tier = item
    return {"collection": collection, "name": name, "tier": tier,
            "format": detect_format(collection)}


with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as ex:
    RESOLVED = list(ex.map(_resolve, SELECTED))

present = [t for t in RESOLVED if t["format"]]
skipped = [t["collection"] for t in RESOLVED if not t["format"]]
if skipped:
    print(f"skipped — no files in S3 ({len(skipped)}): {skipped}")
print(f"ingesting {len(present)} collection(s): "
      f"{sum(t['format'] == 'parquet' for t in present)} parquet, "
      f"{sum(t['format'] == 'csv' for t in present)} csv")

# COMMAND ----------

def start_bronze(t):
    tier = TIER_PARAMS[t["tier"]]  # skynet per-tier batching (max_files + micro-batch interval)
    # TODO (out of scope): switch Auto Loader from directory-listing to file-notification
    # mode (cloudFiles.useNotifications=true + SQS/SNS) to cut S3 LIST cost on the warm loop.
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", t["format"])
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_ROOT}/{DEST_PREFIX}schemas/{t['name']}")
        # addNewColumns: on a new column Auto Loader records the evolved schema then THROWS
        # to force a restart — that restart is EXPECTED, not a failure (see supervise()).
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger", tier["max_files"])
        .option("rescuedDataColumn", "_rescued_data")
    )
    if t["format"] == "csv":
        reader = reader.option("header", "true")

    df = (
        reader.load(source_glob(t["collection"], t["format"]))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_file_epoch_ms",
            F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{13})_", 1).cast("bigint"),
        )
    )
    # TODO (out of scope): enable Delta auto-compaction / optimizeWrite (or a periodic
    # OPTIMIZE + ZORDER) on the bronze tables — the small-file count grows on a warm loop.
    writer = (
        df.writeStream
        .queryName(f"bronze_{t['name']}")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{DEST_PREFIX}bronze/{t['name']}")
        .option("mergeSchema", "true")
    )
    if TRIGGER_MODE == "realtime":
        # batch_duration_seconds from skynet's tier config = the micro-batch cadence.
        writer = writer.trigger(processingTime=f"{tier['interval']} seconds")
    else:
        writer = writer.trigger(availableNow=True)
    return writer.toTable(bronze_table(t["name"]))

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

import json
import socket
import threading
import time as _time
import uuid

RESTART_BACKOFF_SECONDS = 10   # skynet's inter-session pause before a restart
MAX_RESTARTS = 5               # per stream: bounded GENUINE-failure budget (see supervise)
MAX_SCHEMA_RESTARTS = 50       # generous safety cap so an EXPECTED schema-evo restart loop
                               # still terminates instead of spinning forever
HEALTHY_RESET_SECONDS = 300    # a stream that ran healthy this long "earns back" BOTH budgets,
                               # so routine schema drift can't permanently exhaust either cap

STATUS_OK = "ok"

# Auto Loader with schemaEvolutionMode=addNewColumns intentionally FAILS the query when it
# sees a new column (after recording the evolved schema), expecting the caller to restart.
# That restart is EXPECTED behaviour, not a real failure — recognise it and restart.
#
# Classification is NARROW and FAIL-SAFE: we match on the exception TYPE name / Spark
# error-class IDENTIFIER (not loose prose), walk the FULL cause chain, and DEFAULT TO GENUINE
# FAILURE for anything we don't confidently recognise — so an unrelated error can never be
# masked as schema-evolution and retried dozens of times.
#
# VERIFY AT DEPLOY: confirm these exact identifiers against a REAL Auto Loader addNewColumns
# stack trace from your workspace/runtime (throw a new column at one collection and capture
# the raised exception's type + Spark errorClass + the JVM class in the stack). The exact
# wording cannot be verified statically here; if it differs, add the observed identifier
# below. Because the default is "genuine failure", a wrong identifier only means a real
# schema-evo restart is (safely) treated as a failure — never the reverse.
_SCHEMA_EVOLUTION_TYPES = (
    "UnknownFieldException",  # org.apache.spark.sql.catalyst.util.UnknownFieldException (py + JVM)
)
_SCHEMA_EVOLUTION_ERROR_CLASSES = (
    "NEW_FIELDS_IN_RECORD_WITH_FILE_PATH",  # Databricks/Spark error class (best-effort)
)


def _exception_chain(exc):
    """Full cause chain, root-ward, following __cause__ then __context__, cycle-safe."""
    chain, seen, cur = [], set(), exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        chain.append(cur)
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return chain


def _error_identity_signals(exc):
    """Reliable IDENTITY signals for one exception — its Python type name, any Spark
    error-class identifier, and (for Py4J/JVM-wrapped errors) the Java class name and rendered
    stack. Deliberately class/error-class identifiers, not free-text prose."""
    sig = [type(exc).__name__, f"{type(exc).__module__}.{type(exc).__name__}"]
    getec = getattr(exc, "getErrorClass", None)  # pyspark PySparkException / CapturedException
    if callable(getec):
        try:
            v = getec()
            if v:
                sig.append(str(v))
        except Exception:
            pass
    for attr in ("errorClass", "_errorClass"):
        v = getattr(exc, attr, None)
        if v:
            sig.append(str(v))
    java_exc = getattr(exc, "java_exception", None)  # Py4JJavaError wraps the JVM throwable
    if java_exc is not None:
        try:
            sig.append(java_exc.getClass().getName())
        except Exception:
            pass
        try:
            sig.append(str(java_exc))  # JVM message + class-qualified stack
        except Exception:
            pass
    for attr in ("desc", "stackTrace"):  # CapturedException carries the JVM stack here
        v = getattr(exc, attr, None)
        if v:
            sig.append(str(v))
    sig.append(str(exc))  # last resort — the class-qualified name still appears here for JVM errors
    return sig


def _is_schema_evolution(exc):
    """True ONLY if we confidently recognise Auto Loader's expected 'new column -> restart'
    signal by exception TYPE or Spark error-class identifier anywhere in the cause chain.
    Anything else -> False (treated as a genuine failure)."""
    for e in _exception_chain(exc):
        blob = "\n".join(_error_identity_signals(e))
        if any(t in blob for t in _SCHEMA_EVOLUTION_TYPES):
            return True
        blob_up = blob.upper()
        if any(ec in blob_up for ec in _SCHEMA_EVOLUTION_ERROR_CLASSES):
            return True
    return False


def supervise(t):
    """Run one collection's stream, kept alive skynet-style, and REPORT its final state.

    Mirrors consume_topic()'s session loop: run the stream, and if it dies, restart it.
      - availableNow / loop: the query drains its files and returns -> STATUS_OK. A
        schema-evolution failure is expected -> restart (resumes from the checkpoint with
        the evolved schema and finishes draining).
      - realtime: awaitTermination() only returns on a clean stop; on failure we restart.

    Restart accounting (fixes the old "print success, exit 0" bug):
      - Genuine failures burn the MAX_RESTARTS budget; exhausting it returns a FAILED status
        (NOT "ok"), which the caller turns into a non-zero task exit.
      - Schema-evolution restarts are EXPECTED: they always restart (bounded only by the
        generous MAX_SCHEMA_RESTARTS safety cap) and do not burn the genuine-failure budget.
      - A stream that ran healthy for >= HEALTHY_RESET_SECONDS resets the budget, so routine
        schema drift spread over a long-lived stream can't permanently exhaust MAX_RESTARTS.
    """
    restarts = 0          # genuine-failure budget
    schema_restarts = 0   # expected schema-evolution restarts (own safety cap)
    while True:
        started = _time.monotonic()
        try:
            start_bronze(t).awaitTermination()  # drains (availableNow/loop) or blocks (realtime)
            return (t["name"], STATUS_OK)
        except Exception as e:  # noqa: BLE001 — we classify + bound below
            ran = _time.monotonic() - started
            if ran >= HEALTHY_RESET_SECONDS:
                # Earned back BOTH budgets after a healthy run: N legitimate schema changes
                # separated by long healthy periods must not accumulate to permanent failure.
                restarts = 0
                schema_restarts = 0
            msg = str(e).replace("\n", " ")[:160]

            if _is_schema_evolution(e):
                schema_restarts += 1
                if schema_restarts > MAX_SCHEMA_RESTARTS:
                    return (t["name"], f"FAILED: schema-evolution restart loop "
                                       f"({schema_restarts}x, not converging) — {msg}")
                print(f"[{t['name']}] schema evolution (expected) — restart "
                      f"{schema_restarts} in {RESTART_BACKOFF_SECONDS}s: {msg}")
                _time.sleep(RESTART_BACKOFF_SECONDS)
                continue

            # Genuine failure.
            restarts += 1
            if restarts > MAX_RESTARTS:
                return (t["name"], f"FAILED after {restarts - 1} restart(s): {msg}")
            print(f"[{t['name']}] stream failed — restart "
                  f"{restarts}/{MAX_RESTARTS} in {RESTART_BACKOFF_SECONDS}s: {msg}")
            _time.sleep(RESTART_BACKOFF_SECONDS)


def run_wave_drain(items):
    """One bounded availableNow wave: MAX_CONCURRENT streams drain at once, each freeing its
    slot when done, so all collections flow through the small cluster in waves. Returns the
    list of (name, status) results."""
    # TODO (out of scope): give each wave stream its own FAIR scheduler pool
    # (spark.scheduler.mode=FAIR + sc.setLocalProperty("spark.scheduler.pool", ...)) so a big
    # collection can't starve small ones inside a wave.
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as ex:
        return list(ex.map(supervise, items))


def report_and_raise(results, context):
    """Print the per-collection outcome and, if ANY collection failed permanently, RAISE so
    the notebook/task exits non-zero (Jobs retries / alerts fire) instead of printing success."""
    failed = [(n, s) for n, s in results if s != STATUS_OK]
    print(f"{context}: {len(results) - len(failed)} ok, {len(failed)} failed")
    for n, s in failed:
        print(f"  {n}: {s}")
    if failed:
        raise RuntimeError(f"{context}: {len(failed)} collection(s) failed permanently: "
                           f"{[n for n, _ in failed]}")


# --- Single-writer safety: the AUTHORITATIVE guarantee is the Databricks Job config -----------
# RUNBOOK — SINGLE WRITER (read before scheduling this notebook):
#   * The AUTHORITATIVE single-writer guarantee is the Databricks Job setting
#     `max_concurrent_runs=1`. Set it on the Job that runs this notebook. That is what actually
#     prevents two runs of this notebook from writing the same checkpoints at once.
#   * NEVER co-run `loop` mode and a scheduled `availableNow` job (or two runs of ANY mode)
#     against the SAME checkpoint_root. Two writers on one checkpoint corrupt it — run exactly
#     one writer against a given checkpoint_root.
#   * The advisory marker below is ONLY an operator-visibility aid. It has NO heartbeat, NO TTL,
#     NO ownership, and NO enforcement; it never blocks or fails startup and spawns no threads.
#
# WHY NO IN-NOTEBOOK LOCK: we intentionally dropped the earlier in-notebook lock. UC Volumes
# have no atomic compare-and-set, so a read-then-write "lock" cannot be made race-free or
# bulletproof — shipping one would imply a guarantee the platform can't back. We rely on
# max_concurrent_runs=1 plus this advisory marker instead.
#
# The marker is a SIBLING file — it does NOT touch the per-stream checkpoint dirs, so
# checkpoints stay byte-compatible.
ADVISORY_MARKER_PATH = f"{CHECKPOINT_ROOT}/{DEST_PREFIX}writer_advisory.json"
_RUN_ID = uuid.uuid4().hex
try:
    _HOST = socket.gethostname()
except Exception:
    _HOST = "unknown"


def advisory_startup_check():
    """ADVISORY ONLY (best-effort): if a marker from another writer already exists, log a
    PROMINENT warning (with its run_id/host/timestamp if readable), then PROCEED regardless; and
    best-effort write our own marker for operator visibility. NEVER raises, blocks startup, or
    spawns threads — every error is logged and ignored. This is NOT an enforcement mechanism;
    the real single-writer guarantee is the Job's max_concurrent_runs=1."""
    try:
        existing = json.loads(dbutils.fs.head(ADVISORY_MARKER_PATH))
        if isinstance(existing, dict) and existing.get("run_id") not in (None, _RUN_ID):
            print("=" * 88)
            print(f"WARNING: an advisory writer marker already exists at {ADVISORY_MARKER_PATH}")
            print("         ANOTHER WRITER MAY BE ACTIVE against these checkpoints:")
            print(f"           run_id={existing.get('run_id')} host={existing.get('host')} "
                  f"mode={existing.get('mode')} started_at={existing.get('started_at')}")
            print("         If you did NOT intend two writers, STOP one NOW — two writers on the")
            print("         same checkpoints corrupt them. (Advisory only; proceeding. The real")
            print("         guarantee is the Job setting max_concurrent_runs=1.)")
            print("=" * 88)
    except Exception as e:
        # Missing marker (the normal case) or any read error — advisory, so note and proceed.
        print(f"advisory marker: no readable existing marker ({type(e).__name__}) — proceeding")
    try:
        payload = {"run_id": _RUN_ID, "host": _HOST, "mode": TRIGGER_MODE,
                   "started_at": _time.time()}
        dbutils.fs.put(ADVISORY_MARKER_PATH, json.dumps(payload), overwrite=True)
        print(f"advisory writer marker written: run_id={_RUN_ID} host={_HOST} "
              f"mode={TRIGGER_MODE} at {ADVISORY_MARKER_PATH}")
    except Exception as e:
        print(f"advisory marker: could not write ({e}) — proceeding (advisory only)")


def advisory_cleanup():
    """Best-effort remove our advisory marker on clean exit — visibility hygiene only. Never
    raises. Deletes ONLY when we POSITIVELY confirm the marker parses and its run_id == ours;
    on ANY other outcome (not found, read error, malformed/unparseable JSON, missing/mismatched
    run_id) we do NOT delete — so we can never clobber a different writer's marker. (A residual
    read/delete TOCTOU remains and is accepted/documented.)"""
    try:
        existing = json.loads(dbutils.fs.head(ADVISORY_MARKER_PATH))
    except Exception as e:
        # Not found, read error, or unparseable JSON -> can't confirm ownership -> skip deletion.
        print(f"advisory marker: not confirmed ours at cleanup ({type(e).__name__}) — "
              f"leaving any marker untouched")
        return
    if not (isinstance(existing, dict) and existing.get("run_id") == _RUN_ID):
        print("advisory marker: not ours (or no run_id) at cleanup — leaving it intact")
        return
    try:
        dbutils.fs.rm(ADVISORY_MARKER_PATH)
        print(f"advisory writer marker removed: run_id={_RUN_ID}")
    except Exception as e:
        print(f"advisory marker: could not remove ({e}) — harmless (advisory only)")


# NOTE: `present` is resolved once at job start; a warm loop does NOT rediscover collections
# added to S3 mid-run — restart the job/cluster to pick those up (rediscovery is out of scope).
# TODO (out of scope): a StreamingQueryListener for per-batch input-rate / backlog / duration
# metrics instead of the coarse print()s below.

# ALL three modes below WRITE the poc_ checkpoints. The single-writer guarantee is the Job's
# max_concurrent_runs=1 (see runbook above); each mode runs the ADVISORY startup check for
# operator visibility (never blocks) and best-effort clears the advisory marker on exit.

if TRIGGER_MODE == "loop":
    # 24×7 WARM-CLUSTER drain loop. Bounded availableNow wave, sleep, repeat — meant to run
    # continuously on a PERSISTENT/warm classic cluster (NOT a per-run job cluster). Single-writer
    # safety comes from the Job's max_concurrent_runs=1, NOT from this code.
    print(f"warm drain loop: interval={LOOP_INTERVAL_SECONDS}s  max_concurrent={MAX_CONCURRENT}  "
          f"collections={len(present)}")
    advisory_startup_check()
    try:
        _wave = 0
        while True:
            _wave += 1
            results = run_wave_drain(present)
            # A permanently-failing collection MUST break the loop and raise — never swallowed —
            # so the task exits non-zero and Jobs retry/alert fires.
            report_and_raise(results, f"warm drain loop wave {_wave}")
            print(f"wave {_wave} clean — sleeping {LOOP_INTERVAL_SECONDS}s")
            _time.sleep(LOOP_INTERVAL_SECONDS)
    finally:
        advisory_cleanup()

elif TRIGGER_MODE == "availableNow":
    # Once / scheduled drain (original mode, preserved): one bounded wave, then fail the task
    # if any collection failed permanently.
    advisory_startup_check()
    try:
        results = run_wave_drain(present)
        report_and_raise(results, "bronze ingestion")
    finally:
        advisory_cleanup()

else:
    # realtime: one supervisor THREAD per collection (skynet's one-thread-per-topic), all
    # started up front, each keeping its stream alive + restarting on failure. The main
    # thread joins them so the job task stays alive, then fails the task on any permanent failure.
    if len(present) > MAX_CONCURRENT:
        print(f"WARNING: {len(present)} continuous streams on a {MAX_CONCURRENT}-core-ish budget "
              f"will contend on this cluster. Prefer availableNow/loop on a schedule, or shard "
              f"by tier across jobs / use a bigger cluster.")
    _results = {}

    def _run(item):
        _results[item["name"]] = supervise(item)[1]  # distinct keys -> thread-safe under the GIL

    advisory_startup_check()
    try:
        threads = []
        for t in present:
            th = threading.Thread(target=_run, args=(t,), name=f"supervise-{t['name']}", daemon=True)
            th.start()
            threads.append(th)
            _time.sleep(0.1)  # small stagger so 100+ schema inferences don't hit the driver at once
        print(f"{len(threads)} supervisor threads started (one per collection, skynet-style); "
              f"{len(spark.streams.active)} streams active. Blocking until all supervisors exit.")
        for th in threads:
            th.join()
        report_and_raise(list(_results.items()), "realtime supervisors exited")
    finally:
        advisory_cleanup()

# COMMAND ----------

# MAGIC %md ## Row counts (run after an availableNow pass)

# COMMAND ----------

for t in present:
    tbl = bronze_table(t["name"])
    if spark.catalog.tableExists(tbl):
        print(f"{t['name']:<45} bronze rows = {spark.table(tbl).count():>10}")
    else:
        print(f"{t['name']:<45} (no data yet)")
