"""Shared utilities for CrUX BigQuery scripts."""

import os
import sys
from pathlib import Path

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account

# Project root is one level up from scripts/
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(config_path=None):
    """Load settings from YAML config file."""
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "settings.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        print("Copy config/settings.example.yaml to config/settings.yaml and fill in your values.")
        sys.exit(1)

    with open(config_path) as f:
        return yaml.safe_load(f)


def get_credentials(config):
    """Build GCP credentials from service account key."""
    key_path = PROJECT_ROOT / config["gcp"]["service_account_key"]
    if not key_path.exists():
        print(f"ERROR: Service account key not found: {key_path}")
        sys.exit(1)
    return service_account.Credentials.from_service_account_file(str(key_path))


def get_client(config):
    """Create an authenticated BigQuery client."""
    credentials = get_credentials(config)
    return bigquery.Client(
        project=config["gcp"]["project_id"],
        credentials=credentials,
        location=config["gcp"].get("location", "US"),
    )


def get_table_id(config, table_key):
    """Build fully-qualified table ID: project.dataset.table."""
    project = config["gcp"]["project_id"]
    dataset = config["bigquery"]["dataset_name"]
    table = config["bigquery"][table_key]
    return f"{project}.{dataset}.{table}"


def read_sql(filename):
    """Read a SQL file from the sql/ directory."""
    sql_path = PROJECT_ROOT / "sql" / filename
    if not sql_path.exists():
        print(f"ERROR: SQL file not found: {sql_path}")
        sys.exit(1)
    with open(sql_path) as f:
        return f.read()


def format_sql(sql_template, config, **kwargs):
    """Format a SQL template with project/dataset and extra params."""
    return sql_template.format(
        project=config["gcp"]["project_id"],
        dataset=config["bigquery"]["dataset_name"],
        **kwargs,
    )
