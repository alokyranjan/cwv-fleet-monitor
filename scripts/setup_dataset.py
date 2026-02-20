"""Create BigQuery dataset and tables (idempotent).

Usage:
    python scripts/setup_dataset.py [--config path/to/settings.yaml]
"""

import argparse
import sys

from google.cloud import bigquery

from common import load_config, get_client


def create_dataset(client, config):
    """Create the dataset if it doesn't exist."""
    dataset_id = f"{config['gcp']['project_id']}.{config['bigquery']['dataset_name']}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = config["gcp"].get("location", "US")

    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {dataset.full_dataset_id}")
    return dataset


def create_origins_table(client, config):
    """Create the origins table (dealer site registry)."""
    table_id = f"{config['gcp']['project_id']}.{config['bigquery']['dataset_name']}.{config['bigquery']['origins_table']}"

    schema = [
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
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Table ready: {table.full_table_id}")


def create_cwv_monthly_table(client, config):
    """Create the cwv_monthly table with range partitioning and clustering."""
    table_id = f"{config['gcp']['project_id']}.{config['bigquery']['dataset_name']}.{config['bigquery']['cwv_monthly_table']}"

    schema = [
        # Dimensions
        bigquery.SchemaField("yyyymm", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("origin", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("device", "STRING"),
        bigquery.SchemaField("rank", "INT64"),
        # Dealer metadata (denormalized)
        bigquery.SchemaField("dealer_name", "STRING"),
        bigquery.SchemaField("dealer_group", "STRING"),
        bigquery.SchemaField("oem_brand", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("platform_version", "STRING"),
        # P75 values
        bigquery.SchemaField("p75_lcp", "FLOAT64"),
        bigquery.SchemaField("p75_fcp", "FLOAT64"),
        bigquery.SchemaField("p75_inp", "FLOAT64"),
        bigquery.SchemaField("p75_cls", "FLOAT64"),
        bigquery.SchemaField("p75_ttfb", "FLOAT64"),
        # LCP distribution
        bigquery.SchemaField("fast_lcp", "FLOAT64"),
        bigquery.SchemaField("avg_lcp", "FLOAT64"),
        bigquery.SchemaField("slow_lcp", "FLOAT64"),
        # FCP distribution
        bigquery.SchemaField("fast_fcp", "FLOAT64"),
        bigquery.SchemaField("avg_fcp", "FLOAT64"),
        bigquery.SchemaField("slow_fcp", "FLOAT64"),
        # INP distribution
        bigquery.SchemaField("fast_inp", "FLOAT64"),
        bigquery.SchemaField("avg_inp", "FLOAT64"),
        bigquery.SchemaField("slow_inp", "FLOAT64"),
        # CLS distribution
        bigquery.SchemaField("small_cls", "FLOAT64"),
        bigquery.SchemaField("medium_cls", "FLOAT64"),
        bigquery.SchemaField("large_cls", "FLOAT64"),
        # TTFB distribution
        bigquery.SchemaField("fast_ttfb", "FLOAT64"),
        bigquery.SchemaField("avg_ttfb", "FLOAT64"),
        bigquery.SchemaField("slow_ttfb", "FLOAT64"),
        # Device density
        bigquery.SchemaField("desktopDensity", "FLOAT64"),
        bigquery.SchemaField("phoneDensity", "FLOAT64"),
        bigquery.SchemaField("tabletDensity", "FLOAT64"),
        # Navigation types
        bigquery.SchemaField("nav_navigate", "FLOAT64"),
        bigquery.SchemaField("nav_navigate_cache", "FLOAT64"),
        bigquery.SchemaField("nav_reload", "FLOAT64"),
        bigquery.SchemaField("nav_restore", "FLOAT64"),
        bigquery.SchemaField("nav_back_forward", "FLOAT64"),
        bigquery.SchemaField("nav_back_forward_cache", "FLOAT64"),
        bigquery.SchemaField("nav_prerender", "FLOAT64"),
        # RTT
        bigquery.SchemaField("low_rtt", "FLOAT64"),
        bigquery.SchemaField("medium_rtt", "FLOAT64"),
        bigquery.SchemaField("high_rtt", "FLOAT64"),
        # Meta
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    # Range partitioning on yyyymm
    table.range_partitioning = bigquery.RangePartitioning(
        field="yyyymm",
        range_=bigquery.PartitionRange(start=202001, end=203001, interval=1),
    )

    # Clustering
    table.clustering_fields = ["origin", "oem_brand", "region"]

    table = client.create_table(table, exists_ok=True)
    print(f"Table ready: {table.full_table_id}")


def main():
    parser = argparse.ArgumentParser(description="Create BigQuery dataset and tables")
    parser.add_argument("--config", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    print("Setting up BigQuery resources...")
    create_dataset(client, config)
    create_origins_table(client, config)
    create_cwv_monthly_table(client, config)
    print("\nSetup complete.")


if __name__ == "__main__":
    main()
