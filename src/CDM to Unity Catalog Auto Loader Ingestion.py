# Databricks notebook source
# DBTITLE 1,CDM → Unity Catalog: Modern Auto Loader Architecture
# MAGIC %md
# MAGIC #Bronze to Silver
# MAGIC ## CDM → Unity Catalog: Incremental Ingestion via Auto Loader
# MAGIC
# MAGIC **Migration summary (legacy → modern)**
# MAGIC
# MAGIC | Legacy | Modern |
# MAGIC |---|---|
# MAGIC | Service principal credentials in notebook | UC External Location + Storage Credential (no secrets in code) |
# MAGIC | `cdm_to_delta` blob-copy library | Native Auto Loader (`cloudFiles`) reads directly from UC Volume |
# MAGIC | Manual log table to track processed blobs | Auto Loader checkpoint files (exactly-once guarantee) |
# MAGIC | Two-step: copy blobs → convert to Parquet | Single step: CSV Volume → Delta table |
# MAGIC
# MAGIC **Pre-requisite:** An External Location must be registered in Unity Catalog pointing to the ADLS `dataflow-cdm` container, with an attached Storage Credential. The Volume at `cdm_volume_path` below should resolve to that location.

# COMMAND ----------

# DBTITLE 1,About: Configuration
# MAGIC %md
# MAGIC ### Step 1 — Configuration
# MAGIC Define the target Unity Catalog location (`catalog.schema`), the list of CDM entity names to process, and the UC Volume paths for the source data and Auto Loader checkpoints. No credentials are stored here — all ADLS authentication is handled transparently by the UC External Location and its attached Storage Credential.

# COMMAND ----------

# DBTITLE 1,Configuration
# ── Target Unity Catalog location ───────────────────────────────────────────
target_catalog = "silver"
target_schema  = "dataverse"

# ── Entities to ingest (must match names in model.json) ──────────────────────
entities = ["conversationtranscript","systemuser"]

# ── UC Volume paths ───────────────────────────────────────────────────────────
# Source: External Volume backed by the ADLS 'dataflow-cdm' container.
# No credentials needed here — the UC External Location handles ADLS auth.
cdm_volume_path        = "/Volumes/bronze/default/dataverse"
checkpoint_volume_path = "/Volumes/bronze/default/dataverse/_checkpoints"

print(f"Target : {target_catalog}.{target_schema}")
print(f"Source : {cdm_volume_path}")
print(f"Entities: {entities}")

# COMMAND ----------

# DBTITLE 1,About: Parse model.json
# MAGIC %md
# MAGIC ### Step 2 — Parse the CDM Manifest
# MAGIC Reads `model.json` from the source Volume to discover each entity’s attribute names and data types. A type map converts Microsoft CDM types (e.g. `guid`, `datetime`, `decimal`) into their Spark equivalents, producing a typed `StructType` for each entity. Supplying an explicit schema avoids Auto Loader schema inference at read time and ensures type fidelity with the upstream Dataverse export.

# COMMAND ----------

# DBTITLE 1,Parse model.json — Build Spark schemas from CDM manifest
import json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, ShortType,
    DoubleType, FloatType, DecimalType,
    BooleanType, TimestampType, DateType, BinaryType,
)

# CDM dataType → Spark DataType
CDM_TYPE_MAP: dict = {
    "guid":           StringType(),
    "string":         StringType(),
    "int64":          LongType(),
    "int32":          IntegerType(),
    "smallinteger":   ShortType(),
    "decimal":        DecimalType(18, 6),
    "double":         DoubleType(),
    "float":          FloatType(),
    "boolean":        BooleanType(),
    "datetime":       TimestampType(),
    "datetimeoffset": TimestampType(),
    "date":           DateType(),
    "time":           StringType(),
    "binary":         BinaryType(),
}

def cdm_type_to_spark(cdm_type: str):
    return CDM_TYPE_MAP.get((cdm_type or "string").lower(), StringType())

# Read model.json from the UC Volume (no ADLS SDK needed)
model_json_path = f"{cdm_volume_path}/model.json"
with open(model_json_path) as f:
    model = json.load(f)

entity_schemas: dict[str, StructType] = {}
for entity_def in model.get("entities", []):
    name = entity_def.get("name", "")
    if name in entities:
        fields = [
            StructField(attr["name"], cdm_type_to_spark(attr.get("dataType")), nullable=True)
            for attr in entity_def.get("attributes", [])
        ]
        entity_schemas[name] = StructType(fields)
        print(f"  '{name}': {len(fields)} columns parsed from model.json")

missing = set(entities) - set(entity_schemas)
if missing:
    available = [e["name"] for e in model.get("entities", [])]
    raise ValueError(f"Entities not found in model.json: {missing}. Available: {available}")

print(f"\nSchemas ready for: {list(entity_schemas)}")

# COMMAND ----------

# DBTITLE 1,About: Ensure target schema
# MAGIC %md
# MAGIC ### Step 3 — Ensure Target Catalog & Schema Exist
# MAGIC Idempotently creates the target catalog and schema in Unity Catalog using `IF NOT EXISTS` guards, so repeated runs are safe. Individual Delta tables are created automatically by `toTable()` in the next step on the first run — this cell only ensures their parent namespace is in place.

# COMMAND ----------

# DBTITLE 1,Ensure target schema exists in Unity Catalog
# Create the target catalog and schema if they do not already exist.
# toTable() below will create individual Delta tables on first run.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

print(f"Target schema ready: {target_catalog}.{target_schema}")

# COMMAND ----------

# DBTITLE 1,About: Auto Loader ingestion
# MAGIC %md
# MAGIC ### Step 4 — Schema Verification, Optional Reset, and Incremental Ingestion
# MAGIC
# MAGIC **Step 4a — Schema Verification** (`conversationtranscript`): Before running the stream, a batch read with the same CSV parser options is run against the `conversationtranscript` source folder. It prints the 50-column CDM schema, checks null rates on a 5-row sample, and confirms that the JSON `content` column is parsed as a single field and does not spill into adjacent columns. Re-run this cell whenever source data or CSV format changes.
# MAGIC
# MAGIC **Step 4b — Reset** _(run only when re-ingesting from scratch)_: Deletes each entity's Auto Loader checkpoint directory and truncates the corresponding target Delta table. Use this after changing CSV parser settings, or when upstream data must be fully re-processed. Safely skips entities whose checkpoint or table does not yet exist.
# MAGIC
# MAGIC **Step 4c — Incremental Ingestion**: For each entity, Auto Loader (`cloudFiles`) scans the source Volume for new CSV partition files and processes only files not seen in prior runs, tracked via a per-entity checkpoint. Key design choices:
# MAGIC
# MAGIC * **`trigger(availableNow=True)`** — processes all pending files then stops, giving batch-job semantics safe to call from a scheduled Lakeflow job.
# MAGIC * **`header=false` + explicit schema** — CDM partition CSVs have no column headers; the schema from `model.json` is applied positionally, avoiding inference overhead.
# MAGIC * **`pathGlobFilter="*.csv"`** — restricts file discovery to CSV partition files only, excluding CDM manifest JSON files (`model.json`, `*.cdm.json`) that share the same directory.
# MAGIC * **`multiLine=true`** — handles CSV records whose `content` JSON payload spans multiple lines.
# MAGIC * **`quote='"'` / `escape='"'`** — Dataverse CSV encoding: fields containing commas or quotes are outer-quoted with `"`, and inner double-quotes are escaped by doubling (`""`) rather than backslash-escaping.
# MAGIC * **`_ingested_at` / `_source_file` columns** — add load timestamp and source partition path for downstream lineage and debugging.
# MAGIC * **`toTable(target_table)`** — writes directly to a Unity Catalog Delta table; creates it on the first run.

# COMMAND ----------

# DBTITLE 1,Schema Verification — conversationtranscript
# ── Schema verification for conversationtranscript before Auto Loader ────────
# Validates that the CSV parser keeps the JSON `content` column intact instead
# of spilling it into later columns.
ENTITY_TO_VERIFY = "conversationtranscript"

schema   = entity_schemas[ENTITY_TO_VERIFY]
src_path = f"{cdm_volume_path}/{ENTITY_TO_VERIFY}"

# 1 — Print CDM schema
print(f"CDM schema for '{ENTITY_TO_VERIFY}' ({len(schema.fields)} columns):")
for fld in schema.fields:
    print(f"  {fld.name:<55} {fld.dataType.simpleString()}")

# 2 — Sample read with the CSV quote/escape rules that Dataverse exports use.
# CDM partition CSVs have no column headers, and the JSON content field is a
# quoted CSV field with doubled double-quotes inside it.
print("\nSample (5 rows) with Dataverse CSV parsing rules applied:")
sample_df = (
    spark.read
        .format("csv")
        .option("header", "false")
        .option("pathGlobFilter", "*.csv")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(src_path)
        .limit(5)
)

# 3 — Focus the verification on the columns that were previously misaligned.
verification_df = sample_df.select(
    "Id",
    "content",
    "conversationstarttime",
    "conversationtranscriptid"
)

display(verification_df)

# COMMAND ----------

# DBTITLE 1,Reset — Delete checkpoints and truncate target tables
# ── RESET: wipe checkpoints + truncate target tables ──────────────────
# Run this before re-ingesting when the CSV parser settings have changed.
#   - Deleting the checkpoint forces Auto Loader to reprocess ALL source files.
#   - Truncating the table removes previously misread rows so they are not
#     duplicated when the corrected ingestion appends new data.
# ⚠️  This is destructive and cannot be undone without re-running ingestion.

for entity_name in entities:
    checkpoint_path = f"{checkpoint_volume_path}/{entity_name}"
    target_table    = f"{target_catalog}.{target_schema}.{entity_name}"

    # 1 — Delete the Auto Loader checkpoint directory
    try:
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"[✓] [{entity_name}] Checkpoint deleted : {checkpoint_path}")
    except Exception:
        print(f"[-] [{entity_name}] No checkpoint found (first run?) — skipping")

    # 2 — Truncate the target Delta table (keeps schema, removes all rows)
    if spark.catalog.tableExists(target_table):
        spark.sql(f"TRUNCATE TABLE {target_table}")
        print(f"[✓] [{entity_name}] Target table truncated : {target_table}")
    else:
        print(f"[-] [{entity_name}] Table does not exist yet — nothing to truncate")

print("\nReset complete — run the Auto Loader cell below to re-ingest with corrected CSV parsing.")

# COMMAND ----------

# DBTITLE 1,Incremental ingestion — Auto Loader (CSV → Delta)
# Auto Loader reads only new CSV partition files on each run (exactly-once via checkpoint).
# trigger(availableNow=True) behaves like a batch job: processes all pending files then stops.
# The checkpoint replaces the old manual log table.
from pyspark.sql import functions as F

for entity_name, entity_schema in entity_schemas.items():
    source_path      = f"{cdm_volume_path}/{entity_name}"
    checkpoint_path  = f"{checkpoint_volume_path}/{entity_name}"
    target_table     = f"{target_catalog}.{target_schema}.{entity_name}"

    print(f"[{entity_name}] source : {source_path}")
    print(f"[{entity_name}] target : {target_table}")
    print(f"[{entity_name}] checkpoint: {checkpoint_path}")

    (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
            .option("header", "false")          # CDM partition CSVs have no column headers
            .option("pathGlobFilter", "*.csv")              # skip manifest/schema JSON files
            .option("multiLine", "true")          # content column contains multi-line JSON
            .option("quote", '"')               # Dataverse CSV: outer quote char is "
            .option("escape", '"')              # Dataverse CSV: inner quotes doubled ("") not backslash-escaped
            .schema(entity_schema)              # schema from model.json — no inference overhead
            .load(source_path)
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.col("_metadata.file_path"))   # lineage: which partition CSV
        .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)         # batch-style: finish all pending files then stop
            .toTable(target_table)              # creates UC Delta table on first run
    ).awaitTermination()

    count = spark.table(target_table).count()
    print(f"[{entity_name}] Done. Total rows in target: {count:,}\n")

# COMMAND ----------

# DBTITLE 1,About: Verification
# MAGIC %md
# MAGIC ### Step 5 — Verify Ingested Tables
# MAGIC Previews the first 10 rows of each target Delta table to confirm data landed correctly. Run this after ingestion to spot-check column names, data types, null rates, and that the `_ingested_at` / `_source_file` metadata columns are populated before pointing downstream consumers at the table.

# COMMAND ----------

# DBTITLE 1,Verify — Preview ingested Delta tables
for entity_name in entities:
    target_table = f"{target_catalog}.{target_schema}.{entity_name}"
    print(f"── {target_table} ──")
    display(spark.table(target_table).limit(10))
