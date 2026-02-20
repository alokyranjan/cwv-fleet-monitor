"""Estimate BigQuery costs via dry-run queries.

Runs all extraction and dashboard queries in dry-run mode (no data read, no cost)
and reports the estimated bytes scanned and cost at on-demand pricing ($6.25/TB).

Usage:
    python scripts/dry_run.py [--months N] [--config path/to/settings.yaml]
"""

import argparse
import sys

from google.cloud import bigquery

from common import load_config, get_client, get_table_id, read_sql, format_sql


PRICE_PER_TB = 6.25  # BigQuery on-demand pricing (USD)


def fmt_bytes(b):
    """Format bytes into a human-readable string."""
    if b < 1024:
        return f"{b} B"
    elif b < 1024**2:
        return f"{b / 1024:.1f} KB"
    elif b < 1024**3:
        return f"{b / 1024**2:.1f} MB"
    else:
        return f"{b / 1024**3:.2f} GB"


def fmt_cost(b):
    """Calculate and format cost from bytes."""
    cost = (b / 1024**4) * PRICE_PER_TB
    return f"${cost:.4f}"


def dry_run_query(client, sql):
    """Run a dry-run query and return bytes that would be scanned."""
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    return job.total_bytes_processed


def estimate_extraction(client, config):
    """Estimate cost of CrUX extraction queries."""
    print("=" * 70)
    print("EXTRACTION QUERIES (reading from public CrUX table)")
    print("=" * 70)

    sql_template = read_sql("extract_crux_monthly.sql")
    query = format_sql(sql_template, config, target_yyyymm=202601)

    try:
        bytes_scanned = dry_run_query(client, query)
        print(f"\n  Single month extraction:")
        print(f"    Data scanned:  {fmt_bytes(bytes_scanned)}")
        print(f"    Cost:          {fmt_cost(bytes_scanned)}")
        return bytes_scanned
    except Exception as e:
        print(f"\n  ERROR: {e}")
        return 0


def estimate_dashboard_queries(client, config):
    """Estimate cost of Grafana dashboard queries."""
    table = get_table_id(config, "cwv_monthly_table")
    origins_table = get_table_id(config, "origins_table")

    queries = {
        # Fleet Overview
        "FO: LCP Pass Rate": f"""SELECT ROUND(COUNTIF(p75_lcp <= 2500) * 100.0 / COUNT(*), 1) as value
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet')""",

        "FO: INP Pass Rate": f"""SELECT ROUND(COUNTIF(p75_inp <= 200) * 100.0 / COUNT(*), 1) as value
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet')""",

        "FO: CLS Pass Rate": f"""SELECT ROUND(COUNTIF(p75_cls <= 0.10) * 100.0 / COUNT(*), 1) as value
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet')""",

        "FO: FCP Pass Rate": f"""SELECT ROUND(COUNTIF(p75_fcp <= 1800) * 100.0 / COUNT(*), 1) as value
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet')""",

        "FO: TTFB Pass Rate": f"""SELECT ROUND(COUNTIF(p75_ttfb <= 800) * 100.0 / COUNT(*), 1) as value
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet')""",

        "FO: CWV Pass/Fail Pie": f"""SELECT
  COUNTIF(p75_lcp <= 2500 AND p75_inp <= 200 AND p75_cls <= 0.10) as Pass,
  COUNTIF(NOT (p75_lcp <= 2500 AND p75_inp <= 200 AND p75_cls <= 0.10)) as Fail
FROM (
  SELECT origin, AVG(p75_lcp) as p75_lcp, AVG(p75_inp) as p75_inp, AVG(p75_cls) as p75_cls
  FROM `{table}`
  WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
  AND device IN ('phone','desktop','tablet')
  AND p75_lcp IS NOT NULL AND p75_inp IS NOT NULL AND p75_cls IS NOT NULL
  GROUP BY origin
)""",

        "FO: Pass Rate by Device": f"""SELECT device, COUNT(DISTINCT origin) as sites,
  ROUND(COUNTIF(p75_lcp <= 2500) * 100.0 / COUNT(*), 1) as lcp_pass_pct,
  ROUND(COUNTIF(p75_inp <= 200) * 100.0 / COUNT(*), 1) as inp_pass_pct,
  ROUND(COUNTIF(p75_cls <= 0.10) * 100.0 / COUNT(*), 1) as cls_pass_pct,
  ROUND(AVG(p75_lcp), 0) as avg_lcp, ROUND(AVG(p75_inp), 0) as avg_inp, ROUND(AVG(p75_cls), 2) as avg_cls
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND device IS NOT NULL
GROUP BY device ORDER BY device""",

        "FO: CWV Pass Rate Trends": f"""SELECT
  TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(COUNTIF(p75_lcp <= 2500) * 100.0 / COUNT(*), 1) as lcp_good_pct,
  ROUND(COUNTIF(p75_inp <= 200) * 100.0 / COUNT(*), 1) as inp_good_pct,
  ROUND(COUNTIF(p75_cls <= 0.10) * 100.0 / COUNT(*), 1) as cls_good_pct
FROM `{table}` WHERE device IN ('phone','desktop','tablet')
GROUP BY yyyymm ORDER BY yyyymm""",

        "FO: Fleet Avg LCP Trend": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(p75_lcp), 0) as avg_lcp
FROM `{table}` WHERE device IN ('phone','desktop','tablet') AND p75_lcp IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "FO: Fleet Avg INP Trend": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(p75_inp), 0) as avg_inp
FROM `{table}` WHERE device IN ('phone','desktop','tablet') AND p75_inp IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "FO: Fleet Avg CLS Trend": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(p75_cls), 2) as avg_cls
FROM `{table}` WHERE device IN ('phone','desktop','tablet') AND p75_cls IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "FO: Fleet Avg FCP Trend": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(p75_fcp), 0) as avg_fcp
FROM `{table}` WHERE device IN ('phone','desktop','tablet') AND p75_fcp IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "FO: Fleet Avg TTFB Trend": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(p75_ttfb), 0) as avg_ttfb
FROM `{table}` WHERE device IN ('phone','desktop','tablet') AND p75_ttfb IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "FO: OEM Brand Breakdown": f"""SELECT TRIM(brand) as brand,
  ROUND(COUNTIF(p75_lcp <= 2500) * 100.0 / COUNT(*), 1) as lcp_good_pct,
  ROUND(COUNTIF(p75_inp <= 200) * 100.0 / COUNT(*), 1) as inp_good_pct,
  ROUND(COUNTIF(p75_cls <= 0.10) * 100.0 / COUNT(*), 1) as cls_good_pct
FROM `{table}`, UNNEST(SPLIT(oem_brand, ',')) as brand
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet')
AND oem_brand IS NOT NULL AND oem_brand != ''
GROUP BY brand HAVING COUNT(*) >= 10 ORDER BY lcp_good_pct ASC""",

        "FO: Coverage Summary": f"""SELECT COUNT(DISTINCT origin) as origins_with_data,
  (SELECT COUNT(*) FROM `{origins_table}` WHERE is_active = TRUE) as total_origins
FROM `{table}` WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)""",

        # Worst Offenders
        "WO: LCP Distribution": f"""SELECT
  ROUND(AVG(SAFE_DIVIDE(fast_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as good,
  ROUND(AVG(SAFE_DIVIDE(avg_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as needs_improvement,
  ROUND(AVG(SAFE_DIVIDE(slow_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as poor
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND fast_lcp IS NOT NULL""",

        "WO: INP Distribution": f"""SELECT
  ROUND(AVG(SAFE_DIVIDE(fast_inp, fast_inp + avg_inp + slow_inp)) * 100, 1) as good,
  ROUND(AVG(SAFE_DIVIDE(avg_inp, fast_inp + avg_inp + slow_inp)) * 100, 1) as needs_improvement,
  ROUND(AVG(SAFE_DIVIDE(slow_inp, fast_inp + avg_inp + slow_inp)) * 100, 1) as poor
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND fast_inp IS NOT NULL""",

        "WO: CLS Distribution": f"""SELECT
  ROUND(AVG(SAFE_DIVIDE(small_cls, small_cls + medium_cls + large_cls)) * 100, 1) as good,
  ROUND(AVG(SAFE_DIVIDE(medium_cls, small_cls + medium_cls + large_cls)) * 100, 1) as needs_improvement,
  ROUND(AVG(SAFE_DIVIDE(large_cls, small_cls + medium_cls + large_cls)) * 100, 1) as poor
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND small_cls IS NOT NULL""",

        "WO: Worst LCP Top 25": f"""SELECT origin, dealer_name, oem_brand, state, device,
  ROUND(p75_lcp, 0) as p75_lcp, ROUND(p75_inp, 0) as p75_inp, ROUND(p75_cls, 2) as p75_cls
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND p75_lcp IS NOT NULL
ORDER BY p75_lcp DESC LIMIT 25""",

        "WO: Worst INP Top 25": f"""SELECT origin, dealer_name, oem_brand, state, device,
  ROUND(p75_inp, 0) as p75_inp, ROUND(p75_lcp, 0) as p75_lcp, ROUND(p75_cls, 2) as p75_cls
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND p75_inp IS NOT NULL
ORDER BY p75_inp DESC LIMIT 25""",

        "WO: Worst CLS Top 25": f"""SELECT origin, dealer_name, oem_brand, state, device,
  ROUND(p75_cls, 2) as p75_cls, ROUND(p75_lcp, 0) as p75_lcp, ROUND(p75_inp, 0) as p75_inp
FROM `{table}`
WHERE yyyymm = (SELECT MAX(yyyymm) FROM `{table}`)
AND device IN ('phone','desktop','tablet') AND p75_cls IS NOT NULL
ORDER BY p75_cls DESC LIMIT 25""",

        # Site Drilldown (using a sample origin)
        "SD: Site Info": f"""SELECT dealer_name, oem_brand, state, platform_version
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND yyyymm = (SELECT MAX(yyyymm) FROM `{table}` WHERE origin = 'https://www.acadianamazda.com')
LIMIT 1""",

        "SD: LCP Status": f"""SELECT ROUND(AVG(p75_lcp), 0) as p75_lcp_ms
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND device IN ('phone','desktop','tablet')
AND yyyymm = (SELECT MAX(yyyymm) FROM `{table}` WHERE origin = 'https://www.acadianamazda.com')""",

        "SD: INP Status": f"""SELECT ROUND(AVG(p75_inp), 0) as p75_inp_ms
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND device IN ('phone','desktop','tablet')
AND yyyymm = (SELECT MAX(yyyymm) FROM `{table}` WHERE origin = 'https://www.acadianamazda.com')""",

        "SD: CLS Status": f"""SELECT ROUND(AVG(p75_cls), 2) as p75_cls
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND device IN ('phone','desktop','tablet')
AND yyyymm = (SELECT MAX(yyyymm) FROM `{table}` WHERE origin = 'https://www.acadianamazda.com')""",

        "SD: LCP Trend": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(p75_lcp), 0) as p75_lcp
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND device IN ('phone','desktop','tablet') AND p75_lcp IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "SD: LCP Distribution": f"""SELECT TIMESTAMP(PARSE_DATE('%Y%m', CAST(yyyymm AS STRING))) as time,
  ROUND(AVG(SAFE_DIVIDE(fast_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as good,
  ROUND(AVG(SAFE_DIVIDE(avg_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as needs_improvement,
  ROUND(AVG(SAFE_DIVIDE(slow_lcp, fast_lcp + avg_lcp + slow_lcp)) * 100, 1) as poor
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND device IN ('phone','desktop','tablet') AND fast_lcp IS NOT NULL
GROUP BY yyyymm ORDER BY yyyymm""",

        "SD: Device Breakdown": f"""SELECT device,
  ROUND(AVG(p75_lcp), 0) as p75_lcp, ROUND(AVG(p75_inp), 0) as p75_inp,
  ROUND(AVG(p75_cls), 2) as p75_cls, ROUND(AVG(p75_fcp), 0) as p75_fcp,
  ROUND(AVG(p75_ttfb), 0) as p75_ttfb
FROM `{table}`
WHERE origin = 'https://www.acadianamazda.com'
AND yyyymm = (SELECT MAX(yyyymm) FROM `{table}` WHERE origin = 'https://www.acadianamazda.com')
AND device IS NOT NULL GROUP BY device ORDER BY device""",
    }

    print("\n" + "=" * 70)
    print("GRAFANA DASHBOARD QUERIES (reading from cwv_monthly table)")
    print("=" * 70)

    dashboard_groups = {
        "Fleet Overview": "FO:",
        "Worst Offenders": "WO:",
        "Site Drilldown": "SD:",
    }

    grand_total = 0

    for dashboard_name, prefix in dashboard_groups.items():
        print(f"\n  {dashboard_name}:")
        print(f"  {'Query':<35} {'Scanned':<12} {'Cost':<12}")
        print(f"  {'-' * 35} {'-' * 12} {'-' * 12}")

        dashboard_total = 0
        for name, sql in queries.items():
            if not name.startswith(prefix):
                continue
            try:
                b = dry_run_query(client, sql)
                dashboard_total += b
                print(f"  {name:<35} {fmt_bytes(b):<12} {fmt_cost(b):<12}")
            except Exception as e:
                print(f"  {name:<35} ERROR: {e}")

        grand_total += dashboard_total
        print(f"  {'Subtotal':<35} {fmt_bytes(dashboard_total):<12} {fmt_cost(dashboard_total):<12}")

    print(f"\n  {'ALL DASHBOARDS TOTAL':<35} {fmt_bytes(grand_total):<12} {fmt_cost(grand_total):<12}")

    return grand_total


def print_summary(extraction_bytes, dashboard_bytes, backfill_months):
    """Print the final cost summary."""
    print("\n" + "=" * 70)
    print("COST SUMMARY (on-demand pricing: $6.25/TB)")
    print("=" * 70)

    ext_cost = (extraction_bytes / 1024**4) * PRICE_PER_TB
    backfill_cost = ext_cost * backfill_months

    print(f"\n  Extraction:")
    print(f"    Single month:        {fmt_bytes(extraction_bytes):>12}    {fmt_cost(extraction_bytes)}")
    print(f"    {backfill_months}-month backfill:    {fmt_bytes(extraction_bytes * backfill_months):>12}    ${backfill_cost:.2f}")
    print(f"    Ongoing monthly:     {fmt_bytes(extraction_bytes):>12}    {fmt_cost(extraction_bytes)}")

    page_load_cost = (dashboard_bytes / 1024**4) * PRICE_PER_TB
    print(f"\n  Dashboard queries:")
    print(f"    Per page load:       {fmt_bytes(dashboard_bytes):>12}    {fmt_cost(dashboard_bytes)}")
    print(f"    50 loads/day:        {fmt_bytes(dashboard_bytes * 50):>12}    {fmt_cost(dashboard_bytes * 50)}")
    print(f"    50 loads/day/month:  {fmt_bytes(dashboard_bytes * 50 * 30):>12}    ${page_load_cost * 50 * 30:.4f}")

    monthly_total = ext_cost + (page_load_cost * 50 * 30)
    print(f"\n  Estimated monthly total (extraction + 50 loads/day):")
    print(f"    ${monthly_total:.2f}/month")

    print(f"\n  One-time backfill ({backfill_months} months):")
    print(f"    ${backfill_cost:.2f}")

    print(f"\n  Note: If using flat-rate/editions pricing (reserved slots),")
    print(f"  query costs are covered by the reservation — no per-TB charges.")


def main():
    parser = argparse.ArgumentParser(description="Estimate BigQuery costs via dry-run queries")
    parser.add_argument("--months", type=int, default=24,
                        help="Number of months for backfill estimate (default: 24)")
    parser.add_argument("--config", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    client = get_client(config)

    extraction_bytes = estimate_extraction(client, config)
    dashboard_bytes = estimate_dashboard_queries(client, config)
    print_summary(extraction_bytes, dashboard_bytes, args.months)


if __name__ == "__main__":
    main()
