"""Backfill historical CrUX months into cwv_monthly.

Usage:
    python scripts/backfill.py [--start YYYYMM] [--end YYYYMM] [--force] [--config path/to/settings.yaml]
"""

import argparse
import sys

from common import load_config, get_client
from extract_monthly import check_existing_data, delete_month_data, extract_month, print_summary


def generate_months(start_yyyymm, end_yyyymm):
    """Generate list of YYYYMM integers between start and end (inclusive)."""
    months = []
    year = start_yyyymm // 100
    month = start_yyyymm % 100

    while year * 100 + month <= end_yyyymm:
        months.append(year * 100 + month)
        month += 1
        if month > 12:
            month = 1
            year += 1

    return months


def main():
    parser = argparse.ArgumentParser(description="Backfill historical CrUX months")
    parser.add_argument("--start", type=int, help="Start month YYYYMM (default: from config backfill_start)")
    parser.add_argument("--end", type=int, help="End month YYYYMM (default: latest available in CrUX)")
    parser.add_argument("--force", action="store_true", help="Re-extract months that already have data")
    parser.add_argument("--config", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    # Determine range
    start_yyyymm = args.start or config["crux"].get("backfill_start", 202301)

    if args.end:
        end_yyyymm = args.end
    else:
        from extract_monthly import get_latest_crux_month
        print("Auto-detecting latest CrUX month...")
        end_yyyymm = get_latest_crux_month(client, config)
        if not end_yyyymm:
            print("ERROR: Could not detect latest CrUX month")
            sys.exit(1)

    months = generate_months(start_yyyymm, end_yyyymm)
    print(f"Backfill range: {start_yyyymm} to {end_yyyymm} ({len(months)} months)")

    total_rows = 0
    skipped = 0
    errors = 0

    for i, yyyymm in enumerate(months, 1):
        print(f"\n[{i}/{len(months)}] Processing {yyyymm}...")

        # Check existing
        existing = check_existing_data(client, config, yyyymm)
        if existing > 0 and not args.force:
            print(f"  Skipping — {existing} rows already exist")
            skipped += 1
            continue

        if existing > 0 and args.force:
            delete_month_data(client, config, yyyymm)

        try:
            rows = extract_month(client, config, yyyymm)
            if rows:
                total_rows += rows
        except Exception as e:
            print(f"  ERROR: {e}")
            errors += 1

    print(f"\n{'='*60}")
    print(f"Backfill complete.")
    print(f"  Months processed: {len(months) - skipped - errors}")
    print(f"  Months skipped:   {skipped}")
    print(f"  Errors:           {errors}")
    print(f"  Total rows added: {total_rows}")


if __name__ == "__main__":
    main()
