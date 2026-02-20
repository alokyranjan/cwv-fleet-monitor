"""Extract CrUX data for a specific month into cwv_monthly.

Usage:
    python scripts/extract_monthly.py [--month YYYYMM] [--force] [--notify] [--config path/to/settings.yaml]
"""

import argparse
import sys

from common import load_config, get_client, get_table_id, read_sql, format_sql


def get_latest_crux_month(client, config):
    """Auto-detect the latest available month in the CrUX public dataset."""
    source_table = config["crux"]["source_table"]
    query = f"SELECT MAX(yyyymm) as latest FROM `{source_table}`"
    result = client.query(query).result()
    for row in result:
        return row.latest
    return None


def check_existing_data(client, config, target_yyyymm):
    """Check if data already exists for the target month."""
    table_id = get_table_id(config, "cwv_monthly_table")
    query = f"SELECT COUNT(*) as cnt FROM `{table_id}` WHERE yyyymm = {target_yyyymm}"
    result = client.query(query).result()
    for row in result:
        return row.cnt
    return 0


def delete_month_data(client, config, target_yyyymm):
    """Delete existing data for a month (used with --force)."""
    table_id = get_table_id(config, "cwv_monthly_table")
    query = f"DELETE FROM `{table_id}` WHERE yyyymm = {target_yyyymm}"
    job = client.query(query)
    job.result()
    print(f"  Deleted existing data for {target_yyyymm}")


def extract_month(client, config, target_yyyymm):
    """Run the extraction query for a specific month."""
    sql_template = read_sql("extract_crux_monthly.sql")
    query = format_sql(sql_template, config, target_yyyymm=target_yyyymm)

    print(f"  Executing extraction query for {target_yyyymm}...")
    job = client.query(query)
    job.result()

    # Get row count
    rows_inserted = job.num_dml_affected_rows
    print(f"  Rows inserted: {rows_inserted}")
    return rows_inserted


def print_summary(client, config, target_yyyymm):
    """Print a summary of the extracted data."""
    table_id = get_table_id(config, "cwv_monthly_table")
    query = f"""
    SELECT
        device,
        COUNT(*) as origins,
        ROUND(AVG(p75_lcp), 0) as avg_p75_lcp,
        ROUND(AVG(p75_inp), 0) as avg_p75_inp,
        ROUND(AVG(p75_cls), 2) as avg_p75_cls,
        ROUND(AVG(p75_fcp), 0) as avg_p75_fcp,
        ROUND(AVG(p75_ttfb), 0) as avg_p75_ttfb
    FROM `{table_id}`
    WHERE yyyymm = {target_yyyymm}
    GROUP BY device
    ORDER BY device
    """
    result = client.query(query).result()
    print(f"\n  Summary for {target_yyyymm}:")
    print(f"  {'Device':<10} {'Origins':>8} {'LCP':>8} {'INP':>8} {'CLS':>8} {'FCP':>8} {'TTFB':>8}")
    print(f"  {'-'*10} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for row in result:
        def fmt(val, decimals=0):
            if val is None:
                return "     N/A"
            return f"{val:>8.{decimals}f}"
        print(f"  {row.device or 'N/A':<10} {row.origins:>8} {fmt(row.avg_p75_lcp)} {fmt(row.avg_p75_inp)} {fmt(row.avg_p75_cls, 2)} {fmt(row.avg_p75_fcp)} {fmt(row.avg_p75_ttfb)}")


def main():
    parser = argparse.ArgumentParser(description="Extract CrUX data for a specific month")
    parser.add_argument("--month", type=int, help="Target month as YYYYMM (auto-detects latest if omitted)")
    parser.add_argument("--force", action="store_true", help="Re-extract even if data exists")
    parser.add_argument("--notify", action="store_true", help="Post summary to Slack after extraction")
    parser.add_argument("--config", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    # Determine target month
    if args.month:
        target_yyyymm = args.month
    else:
        print("Auto-detecting latest CrUX month...")
        target_yyyymm = get_latest_crux_month(client, config)
        if not target_yyyymm:
            print("ERROR: Could not detect latest CrUX month")
            sys.exit(1)

    print(f"Target month: {target_yyyymm}")

    # Check for existing data
    existing = check_existing_data(client, config, target_yyyymm)
    if existing > 0:
        if args.force:
            print(f"  Found {existing} existing rows — deleting (--force)")
            delete_month_data(client, config, target_yyyymm)
        else:
            print(f"  Data already exists ({existing} rows). Use --force to re-extract.")
            sys.exit(0)

    # Extract
    rows = extract_month(client, config, target_yyyymm)

    if rows and rows > 0:
        print_summary(client, config, target_yyyymm)
    else:
        print("  WARNING: No rows inserted. Check that origins are loaded and match CrUX data.")

    # Slack notification
    if args.notify:
        try:
            from notify_slack import post_notification
            if not post_notification(client, config, target_yyyymm):
                print("  WARNING: Slack notification failed")
        except Exception as e:
            print(f"  WARNING: Slack notification error: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
