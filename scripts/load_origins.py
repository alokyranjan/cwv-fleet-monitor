"""Load dealer origins CSV into BigQuery.

Usage:
    python scripts/load_origins.py [--csv path/to/origins.csv] [--append] [--config path/to/settings.yaml]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from common import load_config, get_client, get_table_id, PROJECT_ROOT


def validate_origins(df):
    """Validate origins DataFrame. Returns (clean_df, errors)."""
    errors = []

    # Check required column
    if "origin" not in df.columns:
        return df, ["Missing required column: origin"]

    # Check https:// prefix
    bad_prefix = df[~df["origin"].str.startswith("https://")]
    if len(bad_prefix) > 0:
        errors.append(f"{len(bad_prefix)} origins missing https:// prefix: {bad_prefix['origin'].tolist()[:5]}")

    # Remove trailing slashes
    df["origin"] = df["origin"].str.rstrip("/")

    # Check duplicates
    dupes = df[df["origin"].duplicated(keep="first")]
    if len(dupes) > 0:
        errors.append(f"{len(dupes)} duplicate origins removed: {dupes['origin'].tolist()[:5]}")
        df = df.drop_duplicates(subset=["origin"], keep="first")

    return df, errors


def load_origins(client, config, csv_path, append=False):
    """Load origins from CSV into BigQuery."""
    table_id = get_table_id(config, "origins_table")

    # Read CSV
    df = pd.read_csv(csv_path)
    print(f"Read {len(df)} rows from {csv_path}")

    # Validate
    df, errors = validate_origins(df)
    for err in errors:
        print(f"  WARNING: {err}")

    # Add timestamps
    df["added_at"] = pd.Timestamp.now(tz="UTC")
    df["updated_at"] = pd.Timestamp.now(tz="UTC")

    # Ensure correct dtypes for string columns (pandas may infer numeric)
    string_cols = ["origin", "dealer_name", "dealer_group", "oem_brand",
                   "region", "state", "platform_version", "tags"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", "")

    # Ensure boolean column
    if "is_active" in df.columns:
        df["is_active"] = df["is_active"].astype(bool)

    # Load to BigQuery
    write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND if append
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=[
            bigquery.SchemaField("origin", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dealer_name", "STRING"),
            bigquery.SchemaField("dealer_group", "STRING"),
            bigquery.SchemaField("oem_brand", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("platform_version", "STRING"),
            bigquery.SchemaField("is_active", "BOOLEAN"),
            bigquery.SchemaField("tags", "STRING"),
            bigquery.SchemaField("added_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion

    table = client.get_table(table_id)
    mode = "appended" if append else "loaded (full reload)"
    print(f"\nSuccessfully {mode} {len(df)} rows into {table_id}")
    print(f"Total rows in table: {table.num_rows}")


def main():
    parser = argparse.ArgumentParser(description="Load dealer origins CSV into BigQuery")
    parser.add_argument("--csv", help="Path to origins CSV file")
    parser.add_argument("--append", action="store_true", help="Append to existing data instead of full reload")
    parser.add_argument("--config", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    csv_path = args.csv or (PROJECT_ROOT / config["origins"]["csv_path"])
    csv_path = Path(csv_path)

    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)

    load_origins(client, config, csv_path, append=args.append)


if __name__ == "__main__":
    main()
