# Databricks notebook source

# COMMAND ----------

"""synthesize_landing_data.py — Refyne-shaped synthetic Hive-partitioned landing zone

Writes synthetic Hive-partitioned parquet/csv files into a UC Volume landing zone
that exactly matches the path contract expected by:
  • bronze_ingest_consumer1.py
  • drain_measurement_harness.py

HOW TO RUN
----------
Attach this file to a classic cluster in the fevm-stable-classic workspace and
run as a Databricks notebook (import via Workspace > Import > Python file).
All parameters are widgets — override them in the notebook UI or via job parameters.

WIDGET LIST
-----------
  catalog         UC catalog name                        (default: stable_classic_7ppxjq_catalog)
  schema          UC schema / namespace                  (default: refyne)
  volume          UC volume name                         (default: landing)
  database        Mongo database segment in path         (default: refyne)
  source_root     Override /Volumes/… root; empty =      (default: /Volumes/<catalog>/<schema>/<volume>)
                  auto-built from catalog/schema/volume
  num_days        Number of day-partitions to generate   (default: 3)
  files_per_day   Files written per collection per day   (default: 5)
  rows_per_file   Rows per file                          (default: 200)
  seed            RNG seed for deterministic output      (default: 42)
  clean_first     Wipe source=mongo subtree before       (default: false)
                  writing — enables idempotent re-runs

OUTPUT
------
After a successful run the final cell prints the exact ``source_root`` value.
Pass it verbatim into both downstream scripts:
  bronze_ingest_consumer1.py   →  source_root  widget
  drain_measurement_harness.py →  source_root  widget
"""

# COMMAND ----------

import csv
import io
import os
import re
import random
from datetime import date, datetime, timedelta, timezone

# COMMAND ----------

dbutils.widgets.text("catalog",       "stable_classic_7ppxjq_catalog")
dbutils.widgets.text("schema",        "refyne")
dbutils.widgets.text("volume",        "landing")
dbutils.widgets.text("database",      "refyne")
dbutils.widgets.text("source_root",   "")          # empty → auto-built
dbutils.widgets.text("num_days",      "3")
dbutils.widgets.text("files_per_day", "5")
dbutils.widgets.text("rows_per_file", "200")
dbutils.widgets.text("seed",          "42")
dbutils.widgets.dropdown("clean_first", "false", ["false", "true"])

# ---------------------------------------------------------------------------
# Input validation — fail CLOSED on any ambiguity.
# ---------------------------------------------------------------------------

_SAFE_TOKEN_RE = re.compile(r'^[A-Za-z0-9_]+$')

def _require_safe_token(name, value):
    """Reject tokens that contain '/', '..', whitespace, or any char outside
    [A-Za-z0-9_].  Prevents path injection through catalog/schema/volume/database."""
    if not _SAFE_TOKEN_RE.fullmatch(value):
        raise ValueError(
            f"widget {name!r}={value!r} must match ^[A-Za-z0-9_]+$ "
            f"(no '/', '..', spaces, or special characters) — refusing to proceed")
    return value


def _require_positive_int(name, raw):
    """Parse raw widget string as an integer and require >= 1."""
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"widget {name!r}={raw!r} must be an integer")
    if v < 1:
        raise ValueError(
            f"widget {name!r} must be a positive integer (>= 1), got {v} — "
            f"zero or negative silently produces no files")
    return v


def _require_contained(source_root, volume_root):
    """Normalize both paths and enforce that source_root equals volume_root or
    is strictly UNDER it with a real separator boundary.

    Prevents sibling-prefix escapes (e.g. volume='landing' but source_root
    points at '/Volumes/.../landing_evil') and '..' traversal.

    Raises ValueError — fail CLOSED — on any case that cannot be proven safe.
    """
    # Reject '..' on the RAW (pre-normalization) segments — normpath would silently
    # collapse them, making a post-normpath check a no-op.  Any '..' anywhere in the
    # user-supplied path is categorically rejected here, regardless of whether it would
    # happen to stay within the volume after normalization.
    raw_parts = source_root.split("/")
    if ".." in raw_parts:
        raise ValueError(
            f"source_root {source_root!r} contains a '..' segment — path traversal "
            f"is not permitted; supply an absolute path with no '..' components")

    # os.path.normpath collapses redundant '/', '/./' sequences.
    norm_src  = os.path.normpath(source_root)
    norm_vol  = os.path.normpath(volume_root)

    # After normalization the path must EQUAL the volume root, or start with
    # the volume root followed by '/' (path boundary).  A bare startswith()
    # check would accept '/Volumes/c/s/landing_evil' for volume 'landing'.
    if norm_src != norm_vol and not norm_src.startswith(norm_vol + "/"):
        raise ValueError(
            f"source_root {source_root!r} (normalized: {norm_src!r}) is not "
            f"contained within the configured volume root {volume_root!r} "
            f"(normalized: {norm_vol!r}) — sibling-prefix or traversal escape "
            f"detected; refusing to proceed")
    # Belt-and-suspenders: confirm no '..' survived into the normalized result either.
    if ".." in norm_src.split("/"):
        raise ValueError(
            f"source_root {source_root!r} still contains '..' after normalization "
            f"({norm_src!r}) — refusing to proceed")


# Validate catalog/schema/volume/database as safe path tokens BEFORE building any path.
CATALOG  = _require_safe_token("catalog",  dbutils.widgets.get("catalog").strip())
SCHEMA   = _require_safe_token("schema",   dbutils.widgets.get("schema").strip())
VOLUME   = _require_safe_token("volume",   dbutils.widgets.get("volume").strip())
DATABASE = _require_safe_token("database", dbutils.widgets.get("database").strip())

# Build the authoritative volume root from the validated tokens.
_VOLUME_ROOT  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# source_root: use override if provided, else auto-build.
_src_override = dbutils.widgets.get("source_root").strip()
if _src_override:
    # Custom source_root must be an absolute /Volumes/... path.
    if not _src_override.startswith("/Volumes/"):
        raise ValueError(
            f"source_root override {_src_override!r} must start with /Volumes/ "
            f"— refusing to write to non-Volume paths")
    SOURCE_ROOT = _src_override
else:
    SOURCE_ROOT = _VOLUME_ROOT

# Normalize + boundary-check: this is the AUTHORITATIVE containment guard.
# Runs regardless of whether source_root was auto-built or overridden.
_require_contained(SOURCE_ROOT, _VOLUME_ROOT)

# Positive-integer guards — zero/negative would silently produce no output.
NUM_DAYS      = _require_positive_int("num_days",      dbutils.widgets.get("num_days"))
FILES_PER_DAY = _require_positive_int("files_per_day", dbutils.widgets.get("files_per_day"))
ROWS_PER_FILE = _require_positive_int("rows_per_file", dbutils.widgets.get("rows_per_file"))

SEED        = int(dbutils.widgets.get("seed"))
CLEAN_FIRST = dbutils.widgets.get("clean_first").lower() == "true"

print(f"source_root   : {SOURCE_ROOT}")
print(f"database      : {DATABASE}")
print(f"num_days      : {NUM_DAYS}")
print(f"files_per_day : {FILES_PER_DAY}")
print(f"rows_per_file : {ROWS_PER_FILE}")
print(f"seed          : {SEED}")
print(f"clean_first   : {CLEAN_FIRST}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Collection catalogue
#
# Names drawn DIRECTLY from GROUP1 in both reference scripts:
#   bronze_ingest_consumer1.py   lines 119-156  (topics have "refyne." prefix)
#   drain_measurement_harness.py lines 134-151  (bare names, same collections)
#
# Selection: 3 high + 2 medium + 1 low = 6 collections
#
#   transactions_statuslogs   ← parent_child: child of 'transactions'.
#                               Exercises the discover_collections() /
#                               child-expansion path in both scripts.
#
#   videokycs                 ← written as CSV.
#                               Exercises detect_format() / format-detection
#                               path in both scripts.
#
#   All others                ← parquet.
# ---------------------------------------------------------------------------
# (collection_name,            tier,     fmt,       parent_or_None)
COLLECTIONS = [
    ("transactions",             "high",   "parquet", None),
    ("transactions_statuslogs",  "high",   "parquet", "transactions"),  # parent_child
    ("users",                    "high",   "parquet", None),
    ("videokycs",                "medium", "csv",     None),            # CSV
    ("departments",              "medium", "parquet", None),
    ("otps",                     "low",    "parquet", None),
]

# COMMAND ----------

# ---------------------------------------------------------------------------
# Per-collection row factories — realistic-ish, lightweight.
#
# Every factory emits a "ts" field as a 13-digit epoch-ms integer, consistent
# with the _file_epoch_ms extraction regex used in bronze_ingest_consumer1.py:
#   regexp_extract(col("_metadata.file_path"), r"/(\d{13})_", 1)  (line 412)
# The file names themselves also start with a 13-digit epoch-ms (see loop below).
# ---------------------------------------------------------------------------

def _jitter_ms(rng, base_ms):
    return base_ms + rng.randint(0, 3_600_000)


def make_transactions(rng, n, base_ms):
    statuses = ["PENDING", "COMPLETED", "FAILED", "REVERSED"]
    return [
        {
            "_id":       f"txn_{rng.randint(100_000, 999_999)}",
            "userId":    f"usr_{rng.randint(10_000, 99_999)}",
            "amount":    round(rng.uniform(10.0, 50_000.0), 2),
            "status":    rng.choice(statuses),
            "createdAt": _jitter_ms(rng, base_ms),
            "updatedAt": _jitter_ms(rng, base_ms),
            "ts":        _jitter_ms(rng, base_ms),
        }
        for _ in range(n)
    ]


def make_transactions_statuslogs(rng, n, base_ms):
    statuses = ["PENDING", "COMPLETED", "FAILED"]
    return [
        {
            "_id":            f"stl_{rng.randint(100_000, 999_999)}",
            "transactionId":  f"txn_{rng.randint(100_000, 999_999)}",
            "previousStatus": rng.choice(statuses),
            "newStatus":      rng.choice(statuses),
            "changedAt":      _jitter_ms(rng, base_ms),
            "ts":             _jitter_ms(rng, base_ms),
        }
        for _ in range(n)
    ]


def make_users(rng, n, base_ms):
    kyc_states = ["PENDING", "APPROVED", "REJECTED"]
    return [
        {
            "_id":        f"usr_{rng.randint(10_000, 99_999)}",
            "employerId": f"emp_{rng.randint(1_000, 9_999)}",
            "kycStatus":  rng.choice(kyc_states),
            "phone":      f"9{rng.randint(100_000_000, 999_999_999)}",
            "createdAt":  _jitter_ms(rng, base_ms),
            "updatedAt":  _jitter_ms(rng, base_ms),
            "ts":         _jitter_ms(rng, base_ms),
        }
        for _ in range(n)
    ]


def make_videokycs(rng, n, base_ms):
    call_states = ["SCHEDULED", "COMPLETED", "MISSED", "CANCELLED"]
    return [
        {
            "_id":          f"vkyc_{rng.randint(100_000, 999_999)}",
            "userId":       f"usr_{rng.randint(10_000, 99_999)}",
            "callStatus":   rng.choice(call_states),
            "attemptCount": rng.randint(1, 5),
            "scheduledAt":  _jitter_ms(rng, base_ms),
            "ts":           _jitter_ms(rng, base_ms),
        }
        for _ in range(n)
    ]


def make_departments(rng, n, base_ms):
    dept_names = ["Engineering", "HR", "Finance", "Operations", "Sales", "Legal"]
    return [
        {
            "_id":        f"dept_{rng.randint(100, 999)}",
            "employerId": f"emp_{rng.randint(1_000, 9_999)}",
            "name":       rng.choice(dept_names),
            "headCount":  rng.randint(5, 500),
            "createdAt":  _jitter_ms(rng, base_ms),
            "ts":         _jitter_ms(rng, base_ms),
        }
        for _ in range(n)
    ]


def make_otps(rng, n, base_ms):
    purposes = ["LOGIN", "WITHDRAWAL", "KYC", "BANK_CHANGE", "DISBURSEMENT"]
    return [
        {
            "_id":       f"otp_{rng.randint(100_000, 999_999)}",
            "phone":     f"9{rng.randint(100_000_000, 999_999_999)}",
            "purpose":   rng.choice(purposes),
            "verified":  rng.choice([True, False]),
            "expiresAt": _jitter_ms(rng, base_ms),
            "ts":        _jitter_ms(rng, base_ms),
        }
        for _ in range(n)
    ]


SCHEMA_FACTORIES = {
    "transactions":            make_transactions,
    "transactions_statuslogs": make_transactions_statuslogs,
    "users":                   make_users,
    "videokycs":               make_videokycs,
    "departments":             make_departments,
    "otps":                    make_otps,
}

# COMMAND ----------

# ---------------------------------------------------------------------------
# Serialisation helpers — Connect-safe: no RDD / sparkContext / _jvm APIs.
# Uses pyarrow (preferred) or pandas (fallback) for parquet; stdlib csv for CSV.
# ---------------------------------------------------------------------------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _HAS_PYARROW = True
except ImportError:
    _HAS_PYARROW = False

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False


def rows_to_parquet_bytes(rows):
    """Serialise list-of-dicts → parquet bytes without touching Spark."""
    if _HAS_PYARROW:
        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()
    if _HAS_PANDAS:
        buf = io.BytesIO()
        try:
            pd.DataFrame(rows).to_parquet(buf, index=False)
        except Exception as exc:
            raise RuntimeError(
                f"pandas.to_parquet failed — parquet backend (pyarrow or fastparquet) "
                f"is missing or broken. Install pyarrow on the cluster "
                f"(pip install pyarrow). Underlying error: {exc}") from exc
        return buf.getvalue()
    raise RuntimeError(
        "Neither pyarrow nor pandas is available — cannot write parquet. "
        "Install one on the cluster (pip install pyarrow) or switch fmt to csv.")


def rows_to_csv_bytes(rows):
    """Serialise list-of-dicts → UTF-8 CSV bytes (with header row)."""
    if not rows:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Volume write helper
# UC Volumes are accessible via POSIX file I/O on both classic and serverless.
# open() + os.makedirs() is Connect-safe (no Spark / JVM dependency).
# ---------------------------------------------------------------------------

def write_bytes(path, data):
    """Write raw bytes to a UC Volume path via POSIX file I/O."""
    parent = os.path.dirname(path)
    try:
        os.makedirs(parent, exist_ok=True)
    except OSError:
        # Fallback: use dbutils on clusters where os.makedirs isn't available
        dbutils.fs.mkdirs(parent)
    with open(path, "wb") as fh:
        fh.write(data)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Optional clean — wipe the source=mongo subtree for idempotent re-runs.
# dbutils.fs.rm is used (not os.rmdir) because it supports recursive deletion.
#
# SAFETY: this block runs AFTER _require_contained() has already proved that
# SOURCE_ROOT is contained within _VOLUME_ROOT.  MONGO_ROOT is SOURCE_ROOT
# with one fixed literal appended, so it is always inside the validated root.
# We never pass an unvalidated path to dbutils.fs.rm.
# ---------------------------------------------------------------------------
MONGO_ROOT = f"{os.path.normpath(SOURCE_ROOT)}/source=mongo"

if CLEAN_FIRST:
    # One final belt-and-suspenders check: normpath the delete target and
    # confirm it still starts with the normpath'd volume root + '/'.
    _norm_mongo = os.path.normpath(MONGO_ROOT)
    _norm_vol   = os.path.normpath(_VOLUME_ROOT)
    if not (_norm_mongo == _norm_vol or _norm_mongo.startswith(_norm_vol + "/")):
        raise ValueError(
            f"SAFETY: clean target {MONGO_ROOT!r} is not inside volume root "
            f"{_VOLUME_ROOT!r} after normalization — refusing to delete")
    try:
        dbutils.fs.rm(MONGO_ROOT, recurse=True)
        print(f"Deleted existing subtree: {MONGO_ROOT}")
    except Exception as exc:
        print(f"Clean skipped (not found or error): {exc}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Main generation loop
#
# Path shape written (matches source_glob() in both reference scripts):
#   {SOURCE_ROOT}/source=mongo/year={YYYY}/month={MM}/day={DD}/
#       database={DATABASE}/collection={name}/{epoch13}_{stem}.{ext}
#
# Verified against:
#   bronze_ingest_consumer1.py   lines 280-283:
#     f"{SOURCE_ROOT}/source=mongo/year=*/month=*/day=*/"
#     f"database={DATABASE}/collection={collection}/*.{fmt}"
#
#   drain_measurement_harness.py lines 265-269:
#     f"{SOURCE_ROOT}/source=mongo/year=*/month=*/day=*/database={DATABASE}/"
#     f"collection={collection}/*.{{{fmt},{fmt}.gz}}"
#
# File names start with a 13-digit epoch-ms, matching the pipeline's
# _file_epoch_ms extraction regex: r"/(\d{13})_"
# ---------------------------------------------------------------------------

# Anchor date: 2025-01-01 UTC.  Epoch ms for that date is 1735689600000 (13 digits).
ANCHOR_DATE     = date(2025, 1, 1)
ANCHOR_DATETIME = datetime(2025, 1, 1, tzinfo=timezone.utc)

rng = random.Random(SEED)

summary_rows = []   # (coll_name, tier, fmt, parent, file_count, byte_count)
total_files  = 0
total_bytes  = 0

for coll_name, tier, fmt, parent in COLLECTIONS:
    factory    = SCHEMA_FACTORIES[coll_name]
    coll_files = 0
    coll_bytes = 0

    for day_idx in range(NUM_DAYS):
        current_date  = ANCHOR_DATE + timedelta(days=day_idx)
        year          = current_date.strftime("%Y")
        month         = current_date.strftime("%m")
        day           = current_date.strftime("%d")

        # Midnight-UTC epoch-ms for this day partition (always 13 digits for 2025+)
        day_anchor_ms = int(
            datetime(current_date.year, current_date.month, current_date.day,
                     tzinfo=timezone.utc).timestamp() * 1000
        )

        coll_dir = (
            f"{SOURCE_ROOT}/source=mongo"
            f"/year={year}/month={month}/day={day}"
            f"/database={DATABASE}/collection={coll_name}"
        )

        # Warn if the collection dir already holds files of a different format —
        # that stale file would confuse the consumers' format-detection probe.
        if not CLEAN_FIRST and os.path.isdir(coll_dir):
            _other_ext = "csv" if fmt == "parquet" else "parquet"
            _stale = [f for f in os.listdir(coll_dir) if f.endswith(f".{_other_ext}")]
            if _stale:
                print(f"  WARNING: {coll_dir} already contains {len(_stale)} "
                      f".{_other_ext} file(s) alongside the expected .{fmt} files — "
                      f"run with clean_first=true to reset format-detection state")

        for file_idx in range(FILES_PER_DAY):
            # 13-digit epoch-ms embedded in the file name — matches pipeline regex
            file_epoch_ms = day_anchor_ms + rng.randint(0, 86_399_999)
            file_stem     = f"{file_epoch_ms:013d}_{coll_name}_{day_idx:02d}_{file_idx:02d}"

            rows = factory(rng, ROWS_PER_FILE, day_anchor_ms)

            if fmt == "parquet":
                data  = rows_to_parquet_bytes(rows)
                fname = f"{file_stem}.parquet"
            else:
                data  = rows_to_csv_bytes(rows)
                fname = f"{file_stem}.csv"

            write_bytes(f"{coll_dir}/{fname}", data)
            coll_files += 1
            coll_bytes += len(data)

    summary_rows.append((coll_name, tier, fmt, parent, coll_files, coll_bytes))
    total_files += coll_files
    total_bytes += coll_bytes
    print(f"  [{tier:6s}]  {coll_name:<30}  {fmt:<7}  "
          f"{coll_files} files  {coll_bytes:,} bytes")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
SEP = "=" * 72
print(f"\n{SEP}")
print("SYNTHESIS COMPLETE")
print(SEP)
print(f"\n  source_root   : {SOURCE_ROOT}")
print(f"  database      : {DATABASE}")
print(f"  total files   : {total_files}")
print(f"  total bytes   : {total_bytes:,}")
print()

col_w = 30
hdr = (f"{'Collection':<{col_w}}  {'Tier':<6}  {'Fmt':<7}  "
       f"{'Files':>5}  {'Bytes':>12}  Note")
print(hdr)
print("-" * len(hdr))
for coll, tier, fmt, parent, nf, nb in summary_rows:
    if parent:
        note = f"parent_child — child of '{parent}'"
    elif fmt == "csv":
        note = "CSV — exercises format-detection path"
    else:
        note = ""
    print(f"{coll:<{col_w}}  {tier:<6}  {fmt:<7}  {nf:>5}  {nb:>12,}  {note}")
print("-" * len(hdr))
print(f"{'TOTAL':<{col_w}}  {'':6}  {'':7}  {total_files:>5}  {total_bytes:>12,}")

print(f"""
Plug this source_root into both pipeline and harness widgets:
  source_root = {SOURCE_ROOT}

Glob produced (matches source_glob() in both reference scripts):
  {SOURCE_ROOT}/source=mongo/year=*/month=*/day=*/database={DATABASE}/collection=<name>/*.<ext>

parent_child collection (exercises discovery/child-expansion):
  collection='transactions_statuslogs'  (parent: 'transactions')

CSV collection (exercises format-detection path):
  collection='videokycs'
""")
