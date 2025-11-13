import os
import re
import json
import logging
from datetime import datetime, timezone
from google.cloud import bigquery

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
DATASET_ID = os.environ.get("BQ_DATASET", "glamira_raw_data")

# Only process files under this prefix in the bucket
ALLOWED_PREFIX = os.environ.get("ALLOWED_PREFIX", "data_in_parquet/")


DEFAULT_SOURCE_FORMAT = os.environ.get("DEFAULT_SOURCE_FORMAT", "PARQUET")

WRITE_DISPOSITION = os.environ.get("WRITE_DISPOSITION", "WRITE_APPEND")

AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "")  # example: "your-project.raw.load_audit"

CSV_SKIP_ROWS = int(os.environ.get("CSV_SKIP_LEADING_ROWS", "1"))
CSV_FIELD_DELIMITER = os.environ.get("CSV_FIELD_DELIMITER", ",")

TABLE_NAMING_MODE = os.environ.get("TABLE_NAMING_MODE", "subfolder")


bq_client = bigquery.Client()

def _dataset_location(project_id: str, dataset_id: str) -> str:
    """Lấy location của dataset để submit job trong đúng vùng."""
    ds = bq_client.get_dataset(f"{project_id}.{dataset_id}")
    return ds.location

def _infer_format(name: str) -> str:
    n = name.lower()
    if n.endswith(".parquet"):
        return "PARQUET"
    if n.endswith(".json") or n.endswith(".jsonl"):
        return "NEWLINE_DELIMITED_JSON"
    if n.endswith(".csv"):
        return "CSV"
    return DEFAULT_SOURCE_FORMAT

def _sanitize_table_component(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", s)

def _table_id_from_path(object_name: str) -> str:
    """
    If TABLE_NAMING_MODE=subfolder:
        ALLOWED_PREFIX/<table>/.../file.parquet -> table
    If TABLE_NAMING_MODE=filename:
        ALLOWED_PREFIX/.../<file>.parquet -> file
    """
    if not object_name.startswith(ALLOWED_PREFIX):
        raise ValueError(f"outside prefix: {object_name}")

    tail = object_name[len(ALLOWED_PREFIX):]  # after prefix
    parts = [p for p in tail.split("/") if p]

    if TABLE_NAMING_MODE.lower() == "subfolder":
        # take first part as table
        if len(parts) == 0:
            raise ValueError("empty key after prefix")
        table = parts[0]
    else:
        # filename mode
        if len(parts) == 0:
            raise ValueError("empty key after prefix")
        file_stem = parts[-1].rsplit(".", 1)[0]
        table = file_stem

    table = _sanitize_table_component(table)
    return f"{PROJECT_ID}.{DATASET_ID}.{table}"

def _load_job_config(source_format: str) -> bigquery.LoadJobConfig:
    cfg = bigquery.LoadJobConfig(
        source_format=getattr(bigquery.SourceFormat, source_format),
        write_disposition=getattr(bigquery.WriteDisposition, WRITE_DISPOSITION),
    )
    
    if source_format in ("NEWLINE_DELIMITED_JSON", "CSV"):
        cfg.autodetect = True
    if source_format == "CSV":
        cfg.skip_leading_rows = CSV_SKIP_ROWS
        cfg.field_delimiter = CSV_FIELD_DELIMITER
        cfg.allow_quoted_newlines = True
        cfg.allow_jagged_rows = True
        cfg.encoding = "UTF-8"
    return cfg

def _stable_job_id(uri: str, table_id: str) -> str:
    # BQ de-dupe based on job_id -> avoid duplicate loads
    return f"gcs2bq_{abs(hash((uri, table_id)))}"

def _insert_audit_row(uri: str, table_id: str, rows: int, status: str, fmt: str, err: str = ""):
    if not AUDIT_TABLE:
        return
    try:
        tbl = bigquery.Table(AUDIT_TABLE)
        now = datetime.now(timezone.utc).isoformat()
        rows_to_insert = [{
            "ts": now,
            "gcs_uri": uri,
            "bq_table": table_id,
            "rows": rows,
            "status": status,   # "SUCCESS" | "FAILED" | "SKIPPED"
            "format": fmt,
            "error": err[:1500] if err else "",
        }]
        bq_client.insert_rows_json(tbl, rows_to_insert, skip_invalid_rows=True)
    except Exception as e:
        logging.warning(f"audit insert failed: {e}")

# Entry point
def trigger_bigquery_load(event, context):
    """
    Background event (Gen2) for GCS finalize:
      event['bucket'], event['name']
    """
    bucket = event.get("bucket")
    name = event.get("name")
    if not bucket or not name:
        logging.info("Missing bucket/name on event.")
        return

    # filter prefix
    if not name.startswith(ALLOWED_PREFIX):
        logging.info(f"Skip (outside prefix): {name}")
        return

    # infer format
    source_format = _infer_format(name)
    if DEFAULT_SOURCE_FORMAT == "PARQUET" and source_format != "PARQUET":
        logging.info(f"Skip (non-parquet while default=PARQUET): {name}")
        _insert_audit_row(f"gs://{bucket}/{name}", "-", 0, "SKIPPED", source_format, "non-parquet")
        return

    try:
        table_id = _table_id_from_path(name)
    except ValueError as e:
        logging.info(f"Skip (table inference): {e}")
        _insert_audit_row(f"gs://{bucket}/{name}", "-", 0, "SKIPPED", source_format, str(e))
        return

    gcs_uri = f"gs://{bucket}/{name}"
    job_config = _load_job_config(source_format)

    try:
        # Submit load job to avoid mismatch region
        location = _dataset_location(PROJECT_ID, DATASET_ID)
        job_id = _stable_job_id(gcs_uri, table_id)

        logging.info(f"Loading {gcs_uri} -> {table_id} as {source_format} (job_id={job_id}, location={location})")
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config,
            job_id=job_id,
            location=location,  # important for multi-region datasets
        )
        res = load_job.result()
        out_rows = getattr(res, "output_rows", 0) or 0
        logging.info(f"Loaded {out_rows} rows to {table_id}")
        _insert_audit_row(gcs_uri, table_id, out_rows, "SUCCESS", source_format, "")
    except Exception as e:
        logging.exception(f"Load FAILED: {gcs_uri} -> {table_id}: {e}")
        _insert_audit_row(gcs_uri, table_id, 0, "FAILED", source_format, str(e))
